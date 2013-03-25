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

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.scheme.SinkCall
import cascading.scheme.SourceCall
import cascading.tap.Tap
import cascading.tuple.Fields
import cascading.tuple.Tuple
import cascading.tuple.TupleEntry
import com.google.common.base.Objects
import org.apache.commons.codec.binary.Base64
import org.apache.commons.lang.SerializationUtils
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.EntityId
import org.kiji.schema.Kiji
import org.kiji.schema.KijiColumnName
import org.kiji.schema.KijiDataRequest
import org.kiji.schema.KijiDataRequestBuilder
import org.kiji.schema.KijiRowData
import org.kiji.schema.KijiTable
import org.kiji.schema.KijiTableWriter
import org.kiji.schema.KijiURI

/**
 * A scheme that can source and sink data from a Kiji table. This scheme is responsible for
 * converting rows from a Kiji table that are input to a Cascading flow into Cascading tuples (see
 * [[org.kiji.chopsticks.KijiScheme#source]]) and writing output
 * data from a Cascading flow to a Kiji table (see [[org.kiji.chopsticks.KijiScheme#sink)]]).
 *
 * @param timeRange for data requested from a Kiji table.
 * @param columns associates tuple field names to Kiji column names.
 */
@ApiAudience.Framework
@ApiStability.Unstable
class KijiScheme(
    private val timeRange: TimeRange,
    private val columns: Map[String, ColumnRequest])
    extends Scheme[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _],
        KijiValue, KijiTableWriter] {
  import KijiScheme._

  /** Fields expected to be in any tuples processed by this scheme. */
  private val fields: Fields = {
    val fieldSpec: Fields = buildFields(columns.keys)

    // Set the fields for this scheme.
    setSourceFields(fieldSpec)
    setSinkFields(fieldSpec)

    fieldSpec
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table.
   *
   * This method gets called on the client machine during job setup. It builds a `KijiDataRequest`
   * that will be used by the job to read data, and sets the input format for the job.
   *
   *@param process is the Cascading flow being built.
   * @param tap that is used with this `Scheme`.
   * @param conf is the configuration used by the Job.
   */
  override def sourceConfInit(
      process: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]],
      conf: JobConf) {
    // Build a data request.
    val request: KijiDataRequest = buildRequest(timeRange, columns.values)

    // Write all the required values to the job's configuration object.
    conf.setInputFormat(classOf[KijiInputFormat])
    conf.set(
        KijiConfKeys.KIJI_INPUT_DATA_REQUEST,
        Base64.encodeBase64String(SerializationUtils.serialize(request)))
  }

  /**
   * Initializes a context for a source that reads from a Kiji table. In this case,
   * the context is a [[org.kiji.chopsticks.KijiValue]] which holds `KijiRowData` read from the
   * Kiji table. This method is called once on the cluster during Job initialization.
   *
   * @param process is the Cascading flow being initialized.
   * @param sourceCall contains the context used with the source.
   */
  override def sourcePrepare(
      process: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiValue, RecordReader[KijiKey, KijiValue]]) {
    sourceCall.setContext(sourceCall.getInput().createValue())
  }

  /**
   * Reads and converts a row from a Kiji table to a Cascading Tuple. This method
   * is called once for each row read in a MapReduce task.
   *
   * @param process is the Cascading flow being executed.
   * @param sourceCall contains the context used with the source.
   * @return `true` if a Kiji table row was successfully read and converted to a Cascading tuple,
   *     `false` if there are no more rows to read.
   */
  override def source(
      process: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiValue, RecordReader[KijiKey, KijiValue]]): Boolean = {
    // Get the current key/value pair.
    val value: KijiValue = sourceCall.getContext()
    if (sourceCall.getInput().next(null, value)) {
      val row: KijiRowData = value.get()
      val result: Tuple = rowToTuple(columns, getSourceFields(), row)

      sourceCall.getIncomingEntry().setTuple(result)
      true
    } else {
      false
    }
  }

  /**
   * Cleans up any resources used during a MapReduce job reading from a Kiji table. It clears the
   * context used by the source. This method is called once for each MapReduce task.
   *
   * @param process is the Cascading flow being run.
   * @param sourceCall contains the context for this source.
   */
  override def sourceCleanup(
      process: FlowProcess[JobConf],
      sourceCall: SourceCall[KijiValue, RecordReader[KijiKey, KijiValue]]) {
    sourceCall.setContext(null)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine during job setup.
   * Currently, there are no configuration options for writing to a Kiji table,
   * so this method is a no-op.
   *
   * @param process is the Cascading flow being built.
   * @param tap that is used with this `Scheme`.
   * @param conf is the configuration used by the Job.
   */
  override def sinkConfInit(
      process: FlowProcess[JobConf],
      tap: Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]],
      conf: JobConf) {
    // No-op since no configuration parameters need to be set to encode data for Kiji.
  }

  /**
   * Initializes any resources needed by a MapReduce job that writes data to a Kiji table. This
   * method is called on the cluster by MapReduce tasks. It initializes a Kiji table writer and
   * sets it as part of the sink's context.
   *
   * @param process is the Cascading flow being run.
   * @param sinkCall contains the context for this sink.
   */
  override def sinkPrepare(
      process: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiTableWriter, OutputCollector[_, _]]) {
    // Open a table writer.
    val uriString: String = process.getConfigCopy().get(KijiConfKeys.KIJI_OUTPUT_TABLE_URI)
    val uri: KijiURI = KijiURI.newBuilder(uriString).build()

    // TODO: Check and see if Kiji.Factory.open should be passed the configuration object in
    //     process.
    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      doAndRelease(kiji.openTable(uri.getTable())) { table: KijiTable =>
        // Set the sink context to an opened KijiTableWriter.
        sinkCall.setContext(table.openTableWriter())
      }
    }
  }

  /**
   * Writes data in a Cascading tuple to columns in a Kiji table. This method is called once per
   * output tuple on the cluster, when a job's output is sinked to a Kiji table.
   *
   * @param process is the Cascading flow being run.
   * @param sinkCall contains the context for this sink (in this case a Kiji table writer).
   */
  override def sink(
      process: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiTableWriter, OutputCollector[_, _]]) {
    // Retrieve writer from the scheme's context.
    val writer: KijiTableWriter = sinkCall.getContext()

    // Write the tuple out.
    val output: TupleEntry = sinkCall.getOutgoingEntry()
    putTuple(columns, getSinkFields(), output, writer)
  }

  /**
   * Releases any resources used by a MapReduce job that writes to a Kiji table. This method is
   * called once by each MapReduce task upon completion. This method retrieves the Kiji table
   * writer used by the MapReduce task and closes it.
   *
   * @param process is the Cascading flow being run.
   * @param sinkCall contains the context for this sink (in this case a Kiji table writer).
   */
  override def sinkCleanup(
      process: FlowProcess[JobConf],
      sinkCall: SinkCall[KijiTableWriter, OutputCollector[_, _]]) {
    // Close the writer.
    sinkCall.getContext().close()
    sinkCall.setContext(null)
  }

  override def equals(other: Any): Boolean = {
    other match {
      case scheme: KijiScheme => columns == scheme.columns
      case _ => false
    }
  }

  override def hashCode(): Int = columns.hashCode()
}

/**
 * Companion to [[org.kiji.chopsticks.KijiScheme]] which contains private methods and values used
 * to implement KijiScheme's functionality.
 */
object KijiScheme {
  /** Field name containing a row's `EntityId`. */
  private[chopsticks] val entityIdField: String = "entityId"

  /**
   * Converts a KijiRowData to a Cascading tuple.
   *
   * @param columns associates tuple field names to column requests.
   * @param fields of a Cascading tuple that should be populated with row data.
   * @param row data used to populate the Cascading tuple.
   * @return a Cascading tuple containing the desired values from the row data.
   */
  private[chopsticks] def rowToTuple(
      columns: Map[String, ColumnRequest],
      fields: Fields,
      row: KijiRowData): Tuple = {
    val result: Tuple = new Tuple()
    val iterator = fields.iterator().asScala

    // Add the row's EntityId to the tuple.
    result.add(row.getEntityId())
    iterator.next()

    // Add the rest.
    iterator.foreach { fieldName =>
      val column: ColumnRequest = columns(fieldName.toString())
      val columnName: KijiColumnName = new KijiColumnName(column.name)

      result.add(row.getValues(columnName.getFamily(), columnName.getQualifier()))
    }

    return result
  }

  /**
   * Writes a Cascading tuple to a Kiji table.
   *
   * @param columns associates tuple field names to column requests.
   * @param fields of a Cascading tuple that contain data to write to Kiji.
   * @param output is a Cascading tuple containing data to write to Kiji.
   * @param writer used to send data to a Kiji table.
   */
  private[chopsticks] def putTuple(
      columns: Map[String, ColumnRequest],
      fields: Fields,
      output: TupleEntry,
      writer: KijiTableWriter) {
    val iterator = fields.iterator().asScala

    // Get the entityId.
    val entityId: EntityId = output.getObject(entityIdField).asInstanceOf[EntityId]
    iterator.next()

    // Store the retrieved columns in the tuple.
    iterator.foreach { fieldName =>
      val column: ColumnRequest = columns(fieldName.toString())
      val columnName: KijiColumnName = new KijiColumnName(column.name)

      writer.put(
          entityId,
          columnName.getFamily(),
          columnName.getQualifier(),
          output.getObject(fieldName.toString()))
    }
  }

  /**
   * Creates a request for data from a Kiji table.
   *
   * @param timeRange to include as part of the data request.
   * @param columns (with options) to be included as part of the data request.
   * @return a data request for the specified columns spanning the specified time range.
   */
  private[chopsticks] def buildRequest(
      timeRange: TimeRange,
      columns: Iterable[ColumnRequest]): KijiDataRequest = {
    def addColumn(builder: KijiDataRequestBuilder, column: ColumnRequest) {
      val columnName: KijiColumnName = new KijiColumnName(column.name)

      builder.newColumnsDef()
          .withMaxVersions(column.maxVersions)
          .withFilter(column.filter)
          .add(columnName)
    }

    val requestBuilder: KijiDataRequestBuilder = KijiDataRequest.builder()
        .withTimeRange(timeRange.begin, timeRange.end)

    columns
        .foldLeft(requestBuilder) { (builder, column) =>
          addColumn(builder, column)
          builder
        }
        .build()
  }

  /**
   * Creates a collection of Cascading tuple fields with the specified names.
   *
   * @param fieldNames for the Cascading tuple fields.
   * @return a collection of Cascading tuple fields with the specified names.
   */
  private[chopsticks] def buildFields(fieldNames: Iterable[String]): Fields = {
    val fieldArray: Array[Fields] = (Seq(entityIdField) ++ fieldNames)
        .map { name: String => new Fields(name) }
        .toArray

    Fields.join(fieldArray: _*)
  }
}
