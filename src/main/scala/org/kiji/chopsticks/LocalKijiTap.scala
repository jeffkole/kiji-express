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

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.util.Properties
import java.util.UUID

import cascading.flow.FlowProcess
import cascading.scheme.Scheme
import cascading.tap.Tap
import cascading.tuple.TupleEntryCollector
import cascading.tuple.TupleEntryIterator
import cascading.tuple.TupleEntrySchemeCollector
import cascading.tuple.TupleEntrySchemeIterator
import com.google.common.base.Objects

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.chopsticks.Resources.doAndRelease
import org.kiji.mapreduce.framework.KijiConfKeys
import org.kiji.schema.Kiji
import org.kiji.schema.KijiURI

/**
 * A `Tap` for reading data from a Kiji table that can be used with Cascading's local runner.
 *
 * Note: Warnings about a missing serialVersionUID are ignored here. When KijiTap is serialized,
 * the result is not persisted anywhere making serialVersionUID unnecessary.
 */
@ApiAudience.Framework
@ApiStability.Unstable
class LocalKijiTap(
    uri: KijiURI,
    private val scheme: LocalKijiScheme)
    extends Tap[Properties, InputStream, OutputStream](
        scheme.asInstanceOf[Scheme[Properties, InputStream, OutputStream, _, _]]) {
  /** The URI of the table to be read through this tap. */
  private val tableUri: String = uri.toString()
  /** A unique identifier for this tap instance. */
  private val id: String = UUID.randomUUID().toString()

  /**
   * Sets any configuration options that are required for running a local job
   * that reads from a Kiji table.
   *
   * @param process is the Cascading flow being built.
   * @param conf is the configuration of the job associated with this tap.
   */
  override def sourceConfInit(
      process: FlowProcess[Properties],
      conf: Properties) {
    // Store the input table.
    conf.setProperty(KijiConfKeys.KIJI_INPUT_TABLE_URI, tableUri);

    super.sourceConfInit(process, conf);
  }

  /**
   * Sets any configuration options that are required for running a local job
   * that writes to a Kiji table.
   *
   * @param process is the Cascading flow being built.
   * @param conf is the configuration of the job associated with this tap.
   */
  override def sinkConfInit(
      process: FlowProcess[Properties],
      conf: Properties) {
    // Store the output table.
    conf.setProperty(KijiConfKeys.KIJI_OUTPUT_TABLE_URI, tableUri);

    super.sinkConfInit(process, conf);
  }

  /**
   * @return a unique identifier for this tap, used by the Cascading flow planner.
   */
  override def getIdentifier(): String = id

  /**
   * Opens and returns a reader for tuple entries obtained from the data store for this tap.
   *
   * @param process is the Cascading flow being used.
   * @param input stream used to read data from the data source.
   * @return an iterator over tuple entries obtained from the data store.
   */
  override def openForRead(
      process: FlowProcess[Properties],
      input: InputStream): TupleEntryIterator = {
    return new TupleEntrySchemeIterator[Properties, InputStream](
        process,
        getScheme(),
        if (null == input) new ByteArrayInputStream(Array()) else input,
        getIdentifier());
  }

  /**
   * Opens and returns a writer for tuple entries to be written to the data store for this tap.
   *
   * @param process is the Cascading flow being used.
   * @param output stream used to output key-value pairs.
   * @return a writer for tuple entries to be sent to the data store.
   */
  override def openForWrite(
      process: FlowProcess[Properties],
      output: OutputStream): TupleEntryCollector = {
    return new TupleEntrySchemeCollector[Properties, OutputStream](
        process,
        getScheme(),
        if (null == output) new ByteArrayOutputStream() else output,
        getIdentifier());
  }

  /**
   * Creates the data store associated with this tap. This method throws an exception because
   * KijiChopsticks does not support creating Kiji tables as part of an analysis pipeline.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return nothing, as this method always throws an exception.
   */
  override def createResource(jobConf: Properties): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support creating tables for you.")
  }

  /**
   * Deletes the data store associated with this tap. This method throws an exception because
   * KijiChopsticks does not support deleting Kiji tables as part of an analysis pipeline.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return nothing, as this method always throws an exception.
   */
  override def deleteResource(jobConf: Properties): Boolean = {
    throw new UnsupportedOperationException("KijiTap does not support deleting tables for you.")
  }

  /**
   * Determines if the resource associated with this tap (a Kiji table) exists.
   *
   * @param jobConf is the configuration being used by a Hadoop job associated with this tap.
   * @return `true` if the Kiji table exists, `false` otherwise.
   */
  override def resourceExists(jobConf: Properties): Boolean = {
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
  override def getModifiedTime(jobConf: Properties): Long = System.currentTimeMillis()

  override def equals(other: Any): Boolean = {
    other match {
      case tap: LocalKijiTap => ((tableUri == tap.tableUri)
          && (scheme == tap.scheme)
          && (id == tap.id))
      case _ => false
    }
  }

  override def hashCode(): Int = Objects.hashCode(tableUri, scheme, id)
}
