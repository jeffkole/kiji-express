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

import java.util.NavigableMap

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.filter.KijiColumnFilter
import org.kiji.schema.filter.RegexQualifierColumnFilter

/**
 * A factory for Scalding `Source`s capable of reading from or writing to a Kiji table. Factory
 * methods are also included for creating column requests needed to create `Source`s.
 */
@ApiAudience.Public
@ApiStability.Unstable
object DSL {
  /**
   * Used to indicate that all versions of a column (or columns in a map-type column family) should
   * be read.
   */
  val all: Int = Integer.MAX_VALUE

  /**
   * Used to indicate that only the latest version of a column (or columns in a map-type column
   * family) should be read.
   */
  val latest: Int = 1

  /**
   * Creates a request for a map-type column family from a Kiji table.
   *
   * Such a request retrieves cell(s) for all columns from the map-type column family, subject
   * to the column's qualifier matching a regular expression.
   *
   * @param name of the map-type column family requested.
   * @param qualifierMatches is a regular expression that qualifiers for columns retrieved must
   *     match. Use the empty string (default) to match all qualifiers / retrieve all columns.
   * @param versions (maximum) of each column in the family to retrieve. The most recent versions
   *     are retrieved first.
   * @return a new request, with the specified parameters, for the map-type column family.
   */
  def MapColumn(
      name: String,
      qualifierMatches: String = "",
      versions: Int = latest): ColumnRequest = {
    require(name.split(":").length == 1)

    val filter: KijiColumnFilter = {
      if ("" == qualifierMatches) {
        null
      } else {
        new RegexQualifierColumnFilter(qualifierMatches)
      }
    }

    new ColumnRequest(name, versions, filter)
  }

  /**
   * Creates a request for a column from a Kiji table.
   *
   * @param name of the column requested, which should be of the form "family:qualifier".
   * @param versions (maximum) of the column to retrieve. The most recent versions are retrieved
   *     first. By default, only the latest version is retrieved.
   * @return a new request, with the specified parameters, for the column.
   */
  def Column(
      name: String,
      versions: Int = latest): ColumnRequest = {
    require(name.split(":").length == 2)

    new ColumnRequest(name, versions, null)
  }

  /**
   * Creates a `Source` capable of reading rows (that include the specified columns) from a Kiji
   * table.
   *
   * Columns are requested by name (of the form "family:qualifier"). Columns requested are
   * subject to the default data request options (no restriction on time range retrieved,
   * only the most recent version retrieved for each column). Only specific columns may be
   * requested with this method, not map-type column families.
   *
   * This method accepts a collection of pairs mapping the names of columns in a Kiji table to
   * the names of desired tuple fields. When the generated `Source` is used as part of an
   * analysis pipeline, one tuple will be generated per Kiji table row retrieved,
   * and column values will be available in the tuple field associated here. The entity id for
   * the row will automatically be available in the tuple field named "entityId".
   *
   * @param tableURI is a Kiji URI that addresses the table to read from.
   * @param columns is a collection of pairs associating the names of Kiji columns (in the form
   *     "family:qualifier") with tuple field names that will be used to access column values in an
   *     analysis pipeline.
   * @return a `Source` that will read rows including the specified columns from the Kiji table.
   */
  def KijiInput(
      tableURI: String) (
        columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Creates a `Source` capable of reading rows (that include the specified columns) from a Kiji
   * table.
   *
   * Columns are requested by name (of the form "family:qualifier"), and only cells that fall
   * within the specified time range will be retrieved from the columns. Only the most recent
   * version of each requested column will be retrieved. Only specific columns may be requested
   * with this method, not map-type column families.
   *
   * This method accepts a collection of pairs mapping the names of columns in a Kiji table to
   * the names of desired tuple fields. When the generated `Source` is used as part of an
   * analysis pipeline, one tuple will be generated per Kiji table row retrieved,
   * and column values will be available in the tuple field associated here. The entity id for
   * the row will automatically be available in the tuple field named "entityId".
   *
   * @param tableURI is a Kiji URI that addresses the table to read from.
   * @param timeRange that retrieved cells must belong to.
   * @param columns is a collection of pairs associating the names of Kiji columns (in the form
   *     "family:qualifier") with tuple field names that will be used to access column values in an
   *     analysis pipeline.
   * @return a `Source` that will read rows including the specified columns from the Kiji table.
   */
  def KijiInput(
      tableURI: String,
      timeRange: TimeRange) (
        columns: (String, Symbol)*): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, timeRange, columnMap)
  }

  /**
   * Creates a `Source` capable of reading rows (that include the specified columns) from a Kiji
   * table.
   *
   * Columns (or map-type column families) are requested using a
   * [[org.kiji.chopsticks.ColumnRequest]] and cells retrieved for each column (or map-type
   * column family) are subject to the restrictions specified in the
   * [[org.kiji.chopsticks.ColumnRequest]]. No restrictions are placed on the time range retrieved.
   *
   * This method accepts a map associating requests for columns in a Kiji table to
   * the names of desired tuple fields. When the generated `Source` is used as part of an
   * analysis pipeline, one tuple will be generated per Kiji table row retrieved,
   * and column values will be available in the tuple field associated here. The entity id for
   * the row will automatically be available in the tuple field named "entityId".
   *
   * @param tableURI is a Kiji URI that addresses the table to read from.
   * @param columns is a map associating requests for columns (or map-type column
   *     families) with tuple field names that will be used to access values in an analysis
   *     pipeline.
   * @return a `Source` that will read rows including the specified columns (or column families)
   *     from the Kiji table.
   */
  def KijiInput(
      tableURI: String,
      columns: Map[ColumnRequest, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Creates a `Source` capable of reading rows (that include the specified columns) from a Kiji
   * table.
   *
   * Columns (or map-type column families) are requested using a
   * [[org.kiji.chopsticks.ColumnRequest]] and cells retrieved for each column (or map-type
   * column family) are subject to the restrictions specified in the
   * [[org.kiji.chopsticks.ColumnRequest]]. Only cells that fall within the specified time range
   * will be retrieved.
   *
   * This method accepts a map associating requests for columns in a Kiji table to
   * the names of desired tuple fields. When the generated `Source` is used as part of an
   * analysis pipeline, one tuple will be generated per Kiji table row retrieved,
   * and column values will be available in the tuple field associated here. The entity id for
   * the row will automatically be available in the tuple field named "entityId".
   *
   * @param tableURI is a Kiji URI that addresses the table to read from.
   * @param timeRange that retrieved cells must belong to.
   * @param columns is a collection of pairs associating requests for columns (or map-type column
   *     families) with tuple field names that will be used to access values in an analysis
   *     pipeline.
   * @return a `Source` that will read rows including the specified columns (or column families)
   *     from the Kiji table.
   */
  def KijiInput(
      tableURI: String,
      timeRange: TimeRange,
      columns: Map[ColumnRequest, Symbol]): KijiSource = {
    val columnMap = columns
        .map { case (col, field) => (field, col) }
    new KijiSource(tableURI, timeRange, columnMap)
  }

  /**
   * Creates a `Source` capable of writing values to a Kiji table. The source created will write
   * the value in a specific tuple field to a cell with the current timestamp in a column in a
   * Kiji table. The row written to will be determined by the entity id present in the tuple
   * field "entityId".
   *
   * @param tableURI is a Kiji URI that addresses the table to write to.
   * @param columns is a collection of pairs associating tuple field names with columns in a Kiji
   *     table. The value from the tuple field will be written to the associated Kiji table column.
   * @return a `Source` that can write values to a Kiji table.
   */
  def KijiOutput(
      tableURI: String) (
        columns: (Symbol, String)*): KijiSource = {
    val columnMap = columns
        .toMap
        .mapValues(Column(_))
    new KijiSource(tableURI, TimeRange.All, columnMap)
  }

  /**
   * Creates a `Source` capable of writing values to a Kiji table. The source created will write
   * the value in a specific tuple field to a cell with the current timestamp in a column in a
   * Kiji table. The row written to will be determined by the entity id present in the tuple
   * field "entityId".
   *
   * @param tableURI is a Kiji URI that address the table to write to.
   * @param columns is a map associating tuple field names with columns in a Kiji table. The value
   *     from the tuple field will be written to the associated Kiji table column.
   * @return a `Source` that can write values to a Kiji table.
   */
  def KijiOutput(
      tableURI: String,
      columns: Map[Symbol, ColumnRequest]): KijiSource = {
    new KijiSource(tableURI, TimeRange.All, columns)
  }

  /**
   * Gets the value of the first entry from a [[java.util.NavigableMap]].
   *
   * @param slice is the map to retrieve the value from.
   * @tparam T is the type of value stored in the map.
   * @return the value of the first entry in the specified map.
   */
  // TODO: This can be removed once KijiSlice is in.
  def getMostRecent[T](slice: NavigableMap[Long, T]): T = slice.firstEntry().getValue()
}
