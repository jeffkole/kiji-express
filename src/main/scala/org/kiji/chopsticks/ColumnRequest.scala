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

import java.io.Serializable

import org.kiji.annotations.ApiAudience
import org.kiji.annotations.ApiStability
import org.kiji.schema.filter.KijiColumnFilter

/**
 * Holds column-level options used to request data from a particular column (or map-type column
 * family) in a Kiji table.
 *
 * @param name of the column (e.g., "family:qualifier") or map-type column family (e.g., "family").
 * @param maxVersions of the column (or for columns in a map-type column family) to be retrieved
 *     from Kiji.
 * @param filter that column values retrieved from Kiji must satisfy.
 */
@ApiAudience.Public
@ApiStability.Unstable
final case class ColumnRequest private[chopsticks] (
    name: String,
    maxVersions: Int = 1,
    // Private to package chopsticks for now. Should be made public when we decide on type
    // to give to users.
    private[chopsticks] val filter: KijiColumnFilter = null) extends Serializable
