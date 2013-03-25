/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.kiji.chopsticks

import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer
import java.io.InputStream
import java.io.OutputStream
import java.util.NavigableMap
import java.util.Properties

import cascading.flow.FlowProcess
import cascading.scheme.ConcreteCall
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import com.twitter.scalding.AccessMode
import com.twitter.scalding.HadoopTest
import com.twitter.scalding.Hdfs
import com.twitter.scalding.Local
import com.twitter.scalding.Mode
import com.twitter.scalding.Read
import com.twitter.scalding.Source
import com.twitter.scalding.Test
import com.twitter.scalding.Write
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources._
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiRowScanner
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableReader
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI

/**
 * A Scalding `Source` which can read data from / write data to a Kiji table.
 *
 * A `KijiSource` is the type clients use to read data from a Kiji table in an analysis pipeline,
 * or write data to Kiji from a pipeline. End-users, however, should not directly instantiate
 * instances of `KijiSource`. Instead, clients should use the methods in the
 * [[org.kiji.chopsticks.DSL]] module to obtain sources properly configured for input or output.
 *
 * @param tableAddress is a Kiji URI that addresses the table to use for input or output.
 * @param timeRange that data read from Kiji using this source must fall into.
 * @param columns associates tuple field names with columns from a Kiji table.
 */
@ApiAudience.Public
@ApiStability.Unstable
final class KijiSource private[chopsticks] (
    private[chopsticks] val tableAddress: String,
    private[chopsticks] val timeRange: TimeRange,
    private[chopsticks] val columns: Map[Symbol, ColumnRequest])
    extends Source {
  import KijiSource._

  /** A type alias for a Scheme that can be used when running against a MapReduce cluster. */
  private type HadoopScheme = Scheme[JobConf, RecordReader[_, _], OutputCollector[_, _], _, _]
  /** The Kiji URI of the Kiji table used for reading or writing. */
  private val tableUri: KijiURI = KijiURI.newBuilder(tableAddress).build()
  /** A Kiji scheme intended to be used with Scalding/Cascading's hdfs mode. */
  private val kijiScheme: KijiScheme = new KijiScheme(timeRange, symbolKeysToStringKeys(columns))
  /** A Kiji scheme intended to be used with Scalding/Cascading's local mode. */
  private val localKijiScheme: LocalKijiScheme = {
    new LocalKijiScheme(timeRange, symbolKeysToStringKeys(columns))
  }

  /**
   * Converts a mapping using `Symbol` keys to one using `String` keys.
   *
   * @param aMap with `Symbol` keys.
   * @return an identical map with `String` keys.
   */
  private def symbolKeysToStringKeys[T](aMap: Map[Symbol, T]): Map[String, T] = {
    aMap.map { case (symbol, column) => (symbol.name, column) }
  }

  /**
   * Populates the Kiji table used by this `Source` with row data.
   *
   * The row data is specified as a collection of Cascading tuples, where each tuple contains a
   * special field named `entityId` that contains the entity id for the intended row,
   * and all other field names correspond to column names in the Kiji table to populate. This
   * method should only be used as part of the testing infrastructure and should never be used by
   * outside clients.
   *
   * @param rows contains the tuples with row data that should be used to populate the table.
   * @param fields from the tuple that will be used to populate the table.
   */
  private def populateTestTable(rows: Buffer[Tuple], fields: Fields) {
    // Open a table writer.
    val writer =
        doAndRelease(Kiji.Factory.open(tableUri)) { kiji =>
          doAndRelease(kiji.openTable(tableUri.getTable())) { table =>
            table.openTableWriter()
          }
        }

    // Write the desired rows to the table.
    try {
      rows.foreach { row: Tuple =>
        val tupleEntry = new TupleEntry(fields, row)
        val iterator = fields.iterator()

        // Get the entity id field.
        val entityIdField = iterator.next().toString()
        val entityId = tupleEntry
            .getObject(entityIdField)
            .asInstanceOf[EntityId]

        // Iterate through fields in the tuple, adding each one.
        while (iterator.hasNext()) {
          val field = iterator.next().toString()
          val columnName = new KijiColumnName(columns(Symbol(field)).name)

          // Get the timeline to be written.
          val timeline: NavigableMap[Long, Any] = tupleEntry.getObject(field)
              .asInstanceOf[NavigableMap[Long, Any]]

          // Write the timeline to the table.
          for (entry <- timeline.asScala) {
            val (key, value) = entry
            writer.put(
                entityId,
                columnName.getFamily(),
                columnName.getQualifier(),
                key,
                value)
          }
        }
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Creates a [[org.kiji.chopsticks.KijiScheme]] that can be used to run jobs on a MapReduce
   * cluster.
   */
  override val hdfsScheme: HadoopScheme = kijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[HadoopScheme]

  /**
   * Creates a [[org.kiji.chopsticks.KijiScheme]] that can be used to run jobs using Cascading's
   * local runner.
   */
  override val localScheme: LocalScheme = localKijiScheme
      // This cast is required due to Scheme being defined with invariant type parameters.
      .asInstanceOf[LocalScheme]

  /**
   * Creates a `Tap` which can be used to read or write data from a Kiji table.
   *
   * @param readOrWrite indicates if the tap should be used for reading or writing.
   * @param mode indicates if the tap will be used with the local job runner or on a MapReduce
   *     cluster.
   *@return a tap to use with this data source.
   */
  override def createTap(readOrWrite: AccessMode)(implicit mode: Mode): Tap[_, _, _] = {
    val tap: Tap[_, _, _] = mode match {
      // Production taps.
      case Hdfs(_,_) => new KijiTap(tableUri, kijiScheme).asInstanceOf[Tap[_, _, _]]
      case Local(_) => new LocalKijiTap(tableUri, localKijiScheme).asInstanceOf[Tap[_, _, _]]

      // Test taps.
      // TODO(CHOP-38): Add support for Hadoop based integration tests.
      case HadoopTest(_, buffers) => {
        readOrWrite match {
          case Read => {
            val scheme = kijiScheme
            populateTestTable(buffers(this), scheme.getSourceFields())

            new KijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
          case Write => {
            val scheme = new TestKijiScheme(buffers(this), timeRange,
                symbolKeysToStringKeys(columns))

            new KijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }
      case Test(buffers) => {
        readOrWrite match {
          // Use Kiji's local tap and scheme when reading.
          case Read => {
            val scheme = localKijiScheme
            populateTestTable(buffers(this), scheme.getSourceFields())

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }

          // After performing a write, use TestLocalKijiScheme to populate the output buffer.
          case Write => {
            val scheme = new TestLocalKijiScheme(buffers(this), timeRange,
                symbolKeysToStringKeys(columns))

            new LocalKijiTap(tableUri, scheme).asInstanceOf[Tap[_, _, _]]
          }
        }
      }

      // Delegate any other tap types to Source's default behaviour.
      case _ => super.createTap(readOrWrite)(mode)
    }

    return tap
  }

  override def equals(other: Any): Boolean = {
    other match {
      case source: KijiSource =>
        Objects.equal(tableAddress, source.tableAddress) &&
        Objects.equal(columns, source.columns)
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableAddress, columns)
}

/**
 * Companion to [[org.kiji.chopsticks.KijiSource]] that contains `Scheme` types used during
 * testing.
 */
object KijiSource {
  /**
   * A local scheme that loads rows from a Kiji table into a buffer once a MapReduce task is
   * complete. This class is only used as part of the testing infrastructure (which allows for
   * clients to verify the rows contained in the buffer as part of testing).
   *
   * @param buffer to fill with rows once a MapReduce job is complete.
   * @param timeRange that data read from the Kiji table should fall into.
   * @param columns associates Scalding tuple field names to Kiji columns.
   */
  private class TestLocalKijiScheme(
      val buffer: Buffer[Tuple],
      timeRange: TimeRange,
      columns: Map[String, ColumnRequest])
      extends LocalKijiScheme(timeRange, columns) {

    /**
     * Initializes resources needed to write data to a Kiji table.
     *
     * @param process is the Cascading flow being built.
     * @param tap that is being used with this scheme.
     * @param conf is the configuration used by the job.
     */
    override def sinkConfInit(
        process: FlowProcess[Properties],
        tap: Tap[Properties, InputStream, OutputStream],
        conf: Properties) {
      super.sinkConfInit(process, tap, conf)

      // Store output uri as input uri.
      tap.sourceConfInit(process, conf)
    }

    /**
     * Cleans-up resources used to write data to a Kiji table. In the case of this test scheme,
     * output rows are used to populate the buffer used by this scheme instance.
     *
     * @param process is the Cascading flow being run.
     * @param sinkCall contains the context for this sink.
     */
    override def sinkCleanup(
        process: FlowProcess[Properties],
        sinkCall: SinkCall[KijiTableWriter, OutputStream]) {
      super.sink(process, sinkCall)

      // Read table into buffer.
      val sourceCall: ConcreteCall[InputContext, InputStream] = new ConcreteCall()
      sourceCall.setIncomingEntry(new TupleEntry())
      sourcePrepare(process, sourceCall)
      while (source(process, sourceCall)) {
        buffer += sourceCall.getIncomingEntry().getTuple()
      }
      sourceCleanup(process, sourceCall)
    }
  }

  /**
   * A scheme that loads rows from a Kiji table into a buffer once a MapReduce task is complete.
   * This class is only used as part of the testing infrastructure (which allows for clients to
   * verify the rows contained in the buffer as part of testing).
   *
   * @param buffer to fill with rows once a MapReduce job is complete.
   * @param timeRange that data read from the Kiji table should fall into.
   * @param columns associates Scalding tuple field names to Kiji columns.
   */
  private class TestKijiScheme(
      val buffer: Buffer[Tuple],
      timeRange: TimeRange,
      columns: Map[String, ColumnRequest])
      extends KijiScheme(timeRange, columns) {
    override def sinkCleanup(
        process: FlowProcess[JobConf],
        sinkCall: SinkCall[KijiTableWriter, OutputCollector[_, _]]) {
      super.sink(process, sinkCall)

      // Store the output table.
      val uri: KijiURI = KijiURI
          .newBuilder(process.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI))
          .build()

      // Read table into buffer.
      doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
        doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
          doAndClose(table.openTableReader()) { reader: KijiTableReader =>
            val request: KijiDataRequest = KijiScheme.buildRequest(timeRange, columns.values)
            doAndClose(reader.getScanner(request)) { scanner: KijiRowScanner =>
              val rows: Iterator[KijiRowData] = scanner.iterator().asScala

              rows.foreach { row: KijiRowData =>
                buffer += KijiScheme.rowToTuple(columns, getSinkFields(), row)
              }
            }
          }
        }
      }
    }
  }
}
