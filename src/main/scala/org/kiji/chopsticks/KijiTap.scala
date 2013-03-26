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
import java.io.File
import java.util.UUID

import cascading.flow.FlowProcess
import cascading.property.ConfigDef
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeCollector
import cascading.tap.hadoop.io.HadoopTupleEntrySchemeIterator
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import com.google.common.base.Objects

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.RecordReader
import org.apache.hadoop.mapred.lib.NullOutputFormat
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.DistributedCacheJars
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.mapreduce.produce.KijiProducer
import org.kiji.mapreduce.util.Jars
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * A `Tap` for reading or writing data from a Kiji table. A tap represents a particular data
 * source/sink that can be read from / written to during a MapReduce job. The tap is responsible
 * for configuring a MapReduce job with the correct input/output formats as well as the proper
 * classpath dependencies for MapReduce tasks.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiTap is serialized,
 * the result is not persisted anywhere making serialVersionUID unnecessary.
 *
 * @param uri for the Kiji table this tap will be used to read.
 * @param scheme to be used with this tap that will convert data read from Kiji into Cascading's
 *     tuple model. Note: You must use [[org.kiji.chopsticks.KijiScheme]] with this tap.
 */
@ApiAudience.Framework
@ApiStability.Unstable
class KijiTap(
    uri: KijiURI,
    private val scheme: KijiScheme)
    extends Tap[JobConf, RecordReader[KijiKey, KijiValue], OutputCollector[_, _]](
        scheme.asInstanceOf[Scheme[JobConf, RecordReader[KijiKey, KijiValue],
            OutputCollector[_, _], _, _]]) {
  import KijiTap._

  private val tableUri: String = uri.toString()
  private val id: String = UUID.randomUUID().toString()

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that reads from a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process is the Cascading flow being built.
   * @param conf is the configuration used by the job.
   */
  override def sourceConfInit(process: FlowProcess[JobConf], conf: JobConf) {
    // Configure the job's input format.
    conf.setInputFormat(classOf[KijiInputFormat])

    // Store the input table.
    conf.set(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableUri)

    // Put Kiji dependency jars on the distributed cache.
    getStepConfigDef().setProperty(
        ConfigDef.Mode.UPDATE,
        "tmpjars",
        findKijiJars(conf).reduce { (a, b) => a + "," + b })

    super.sourceConfInit(process, conf)
  }

  /**
   * Sets any configuration options that are required for running a MapReduce job
   * that writes to a Kiji table. This method gets called on the client machine
   * during job setup.
   *
   * @param process is the Cascading flow being built.
   * @param conf is configuration used by the job.
   */
  override def sinkConfInit(process: FlowProcess[JobConf], conf: JobConf) {
    // TODO(CHOP-35): Use an output format that writes to HFiles.
    // Configure the job's output format.
    conf.setOutputFormat(classOf[NullOutputFormat[_, _]])

    // Store the output table.
    conf.set(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri)

    // Put Kiji dependency jars on the distributed cache.
    getStepConfigDef().setProperty(
        ConfigDef.Mode.UPDATE,
        "tmpjars",
        findKijiJars(conf).reduce { (a, b) => a + "," + b })

    super.sinkConfInit(process, conf)
  }

  /**
   * @return a unique identifier for this tap instance, used by the Cascading planner.
   */
  override def getIdentifier(): String = id

  /**
   * Opens and returns a reader for tuple entries obtained from the data store for this tap.
   *
   * @param process is the Cascading flow being used.
   * @param recordReader used to read data from the data source.
   * @return an iterator over tuple entries obtained from the data store.
   */
  override def  openForRead(
      process: FlowProcess[JobConf],
      recordReader: RecordReader[KijiKey, KijiValue]): TupleEntryIterator = {
    new HadoopTupleEntrySchemeIterator(
        process,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        recordReader)
  }

  /**
   * Opens and returns a writer for tuple entries to be written to the data store for this tap.
   *
   * @param process is the Cascading flow being used.
   * @param outputCollector used by MapReduce to output key-value pairs.
   * @return a writer for tuple entries to be sent to the data store.
   */
  override def openForWrite(
      process: FlowProcess[JobConf],
      outputCollector: OutputCollector[_, _]): TupleEntryCollector = {
    new HadoopTupleEntrySchemeCollector(
        process,
        this.asInstanceOf[Tap[JobConf, RecordReader[_, _], OutputCollector[_, _]]],
        outputCollector)
  }

  /**
   * Creates the data store associated with this tap. This method throws an exception because
   * KijiChopsticks does not support creating Kiji tables as part of an analysis pipeline.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return nothing, as this method always throws an exception.
   */
  override def createResource(jobConf: JobConf): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.")
  }

  /**
   * Deletes the data store associated with this tap. This method throws an exception because
   * KijiChopsticks does not support deleting Kiji tables as part of an analysis pipeline.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return nothing, as this method always throws an exception.
   */
  override def deleteResource(jobConf: JobConf): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.")
  }

  /**
   * Determines if the resource associated with this tap (a Kiji table) exists.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return `true` if the Kiji table exists, `false` otherwise.
   */
  override def resourceExists(jobConf: JobConf): Boolean = {
    val uri: KijiURI = KijiURI.newBuilder(tableUri).build()

    doAndRelease(Kiji.Factory.open(uri)) { kiji: Kiji =>
      kiji.getTableNames().contains(uri.getTable())
    }
  }

  /**
   * Gets the last time the resource associated with this tap (a Kiji table) was modified.
   * Currently, this method just returns the current time.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return the current time in milliseconds.
   */
  override def getModifiedTime(jobConf: JobConf): Long = System.currentTimeMillis()

  override def equals(other: Any): Boolean = {
    other match {
      case tap: KijiTap => (tableUri == tap.tableUri) && (scheme == tap.scheme) && (id == tap.id)
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)
}

/**
 * Companion for [[org.kiji.chopsticks.KijiTap]] containing helper methods for adding
 * dependencies to the distributed cache.
 */
object KijiTap {
  private val logger: Logger = LoggerFactory.getLogger(classOf[KijiTap])

  /**
   * Gets the path to the jar file containing a specific class.
   *
   * @param clazz whose jar file should be found.
   * @param libName of the library provided by the jar being searched for. Used only for debug
   *     messages.
   * @return the path to the jar file containing the specified class.
   */
  private def findJarForClass(clazz: Class[_], libName: String): Option[Path] = {
    // Try to find the jar for this class.
    try {
      val jarFile: File = new File(Jars.getJarPathForClass(clazz))
      val jarPath: Path = new Path(jarFile.toURI().toString())
      logger.debug("Found %s jar: %s".format(libName, jarPath))

      Some(jarPath)
    } catch {
      case e => {
        logger.warn("Could not find jar for %s.".format(libName)
            + " Its dependency jars will not be loaded into the distributed cache.")
        logger.debug("Failed to find jar for %s with exception: %s".format(libName, e))

        None
      }
    }
  }

  /**
   * Finds Kiji dependency jars and returns a list of their paths. Use this method to find
   * jars that need to be sent to Hadoop's DistributedCache.
   *
   * @param fsConf is the configuration used to obtain a filesystem to search for jars.
   * @return a list of paths to Kiji dependency jars that should be added to the DistributedCache
   *     of jobs.
   */
  private def findKijiJars(fsConf: Configuration): Seq[String] = {
    // Find the kiji jars.
    val kijiJars: Seq[Path] = {
      val schemaJar: Option[Path] = findJarForClass(classOf[Kiji], "kiji-schema")
      val mapreduceJar: Option[Path] = findJarForClass(classOf[KijiProducer], "kiji-mapreduce")
      val chopsticksJar: Option[Path] = findJarForClass(classOf[KijiTap], "kiji-chopsticks")

      schemaJar.toSeq ++ mapreduceJar.toSeq ++ chopsticksJar.toSeq
    }

    // Add all dependency jars from the directories containing the kiji-schema, kiji-mapreduce,
    // and kiji-chopsticks jars.
    val dependencyJars: Seq[Path] = kijiJars.flatMap { jar: Path =>
      DistributedCacheJars.listJarFilesFromDirectory(fsConf, jar.getParent()).asScala
    }

    // Remove duplicate jars and return.
    dependencyJars
        // Group jar paths by jar name.
        .groupBy { path => path.getName() }
        // Select the first found jar for each jar name.
        .map { case (_, paths) => paths(0).toString() }
        .toSeq
  }
}
