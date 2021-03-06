/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors;

import io.crate.core.collections.Row;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ColumnIndexWriterProjector extends AbstractIndexWriterProjector {

    protected ColumnIndexWriterProjector(ClusterService clusterService,
                                         Settings settings,
                                         TransportActionProvider transportActionProvider,
                                         BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                         TableIdent tableIdent,
                                         @Nullable String partitionIdent,
                                         List<ColumnIdent> primaryKeyIdents,
                                         List<Symbol> primaryKeySymbols,
                                         List<Input<?>> partitionedByInputs,
                                         @Nullable Symbol routingSymbol,
                                         ColumnIdent clusteredByColumn,
                                         List<Reference> columnReferences,
                                         List<Symbol> columnSymbols,
                                         CollectExpression<Row, ?>[] collectExpressions,
                                         @Nullable
                                         Map<Reference, Symbol> updateAssignments,
                                         @Nullable Integer bulkActions,
                                         boolean autoCreateIndices,
                                         UUID jobId) {
        super(bulkRetryCoordinatorPool,
                transportActionProvider,
                partitionIdent,
                primaryKeyIdents,
                primaryKeySymbols,
                partitionedByInputs,
                routingSymbol,
                clusteredByColumn,
                collectExpressions,
                tableIdent,
                jobId
        );
        assert columnReferences.size() == columnSymbols.size();

        Map<Reference, Symbol> insertAssignments = new HashMap<>(columnReferences.size());
        for (int i = 0; i < columnReferences.size(); i++) {
            insertAssignments.put(columnReferences.get(i), columnSymbols.get(i));
        }

        createBulkShardProcessor(
                clusterService,
                settings,
                transportActionProvider.transportBulkCreateIndicesAction(),
                bulkActions,
                autoCreateIndices,
                false, // overwriteDuplicates
                updateAssignments,
                insertAssignments,
                jobId);
    }

    @Override
    protected Row updateRow(Row row) {
        return row;
    }
}
