/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.operation.collect;

import io.crate.operation.RowDownstream;
import io.crate.planner.node.dql.CollectPhase;

import java.util.Collection;

public interface CollectOperation {

    /**
     * Collecting data from a local source (like shards, a file, ...)
     * and feeding it to the <code>downstream</code>.
     * @param collectNode CollectNode defining what to collect and how to process it
     * @param downstream the final downstream to send the collected and processed rows to.
     * @return a list of CrateCollectors, one for each started collect execution (e.g. 1 for each shard)
     */
    Collection<CrateCollector> collect(CollectPhase collectNode, RowDownstream downstream, JobCollectContext jobCollectContext);
}
