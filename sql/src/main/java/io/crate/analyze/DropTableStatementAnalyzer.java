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

package io.crate.analyze;

import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.sql.tree.DefaultTraversalVisitor;
import io.crate.sql.tree.DropTable;
import io.crate.sql.tree.Node;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

@Singleton
public class DropTableStatementAnalyzer extends DefaultTraversalVisitor<DropTableAnalyzedStatement, Analysis> {

    private final Schemas schemas;

    @Inject
    protected DropTableStatementAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    @Override
    public DropTableAnalyzedStatement visitDropTable(DropTable node, Analysis context) {
        DropTableAnalyzedStatement statement = new DropTableAnalyzedStatement(schemas, node.ignoreNonExistentTable());
        statement.table(TableIdent.of(node.table(), context.parameterContext().defaultSchema()));
        return statement;
    }

    public AnalyzedStatement analyze(Node node, Analysis analysis) {
        analysis.expectsAffectedRows(true);
        return process(node, analysis);
    }
}
