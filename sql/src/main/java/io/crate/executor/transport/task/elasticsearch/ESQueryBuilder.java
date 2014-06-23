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

package io.crate.executor.transport.task.elasticsearch;


import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.crate.analyze.WhereClause;
import io.crate.executor.transport.task.elasticsearch.facet.UpdateFacet;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.operation.Input;
import io.crate.operation.operator.*;
import io.crate.operation.operator.any.*;
import io.crate.operation.predicate.IsNullPredicate;
import io.crate.operation.predicate.NotPredicate;
import io.crate.operation.scalar.MatchFunction;
import io.crate.operation.scalar.geo.DistanceFunction;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.ESSearchNode;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

/**
 * ESQueryBuilder: Use to convert a whereClause to XContent or a ESSearchNode to XContent
 */
public class ESQueryBuilder {

    private final static Visitor visitor = new Visitor();
    private final static OrderBySymbolVisitor orderByVisitor = new OrderBySymbolVisitor();

    /**
     * adds the "query" part to the XContentBuilder
     */

    private void whereClause(Context context, WhereClause whereClause) throws IOException {
        context.builder.startObject("query");
        if (whereClause.hasQuery()) {
            visitor.process(whereClause.query(), context);
        } else {
            context.builder.field("match_all", new HashMap<>());

        }
        context.builder.endObject();

    }

    /**
     * use to generate the "query" xcontent
     */
    public BytesReference convert(WhereClause whereClause) throws IOException {
        Context context = new Context();
        context.builder = XContentFactory.jsonBuilder().startObject();
        whereClause(context, whereClause);
        context.builder.endObject();
        return context.builder.bytes();
    }

    static Set<String> commonAncestors(List<String> fields){
        int idx = 0;
        String previous = null;

        Collections.sort(fields);
        Set<String> result = new HashSet<>(fields.size());
        for (String field : fields) {
            if (idx>0){
                if (!field.startsWith(previous + '.')){
                    previous = field;
                    result.add(field);
                }
            } else {
                result.add(field);
                previous = field;
            }
            idx++;
        }
        return result;
    }

    /**
     * use to create a full elasticsearch query "statement" including fields, size, etc.
     */
    public BytesReference convert(ESSearchNode node) throws IOException {
        assert node != null;
        List<? extends Reference> outputs;

        outputs = node.outputs();
        Context context = new Context();
        context.builder = XContentFactory.jsonBuilder().startObject();
        XContentBuilder builder = context.builder;

        List<String> fields = new ArrayList<>(outputs.size());
        boolean needWholeSource = false;
        for (Reference output : outputs) {
            ColumnIdent columnIdent = output.info().ident().columnIdent();
            if (columnIdent.isSystemColumn()){
                if (DocSysColumns.VERSION.equals(columnIdent)){
                    builder.field("version", true);
                } else if (DocSysColumns.RAW.equals(columnIdent)|| DocSysColumns.DOC.equals(columnIdent)){
                    needWholeSource = true;
                }
            } else if (node.partitionBy().indexOf(output.info()) < 0) { // do not include partitioned by columns
                fields.add(columnIdent.fqn());
            }
        }

        if (!needWholeSource){
            if (fields.size() > 0){
                builder.startObject("_source");
                builder.field("include", commonAncestors(fields));
                builder.endObject();
            } else {
                builder.field("_source", false);
            }
        }
        whereClause(context, node.whereClause());

        if (context.ignoredFields.containsKey("_score")) {
            builder.field("min_score", ((Number) context.ignoredFields.get("_score")).doubleValue());
        }

        addSorting(node.orderBy(), node.reverseFlags(), node.nullsFirst(), context.builder);

        builder.field("from", node.offset());
        builder.field("size", node.limit());

        builder.endObject();
        return builder.bytes();
    }

    /**
     * use to create a full elasticsearch query "statement" used by deleteByQuery actions.
     */
    public BytesReference convert(ESDeleteByQueryNode node) throws IOException {
        assert node != null;

        Context context = new Context();
        context.builder = XContentFactory.jsonBuilder().startObject();
        XContentBuilder builder = context.builder;

        whereClause(context, node.whereClause());

        builder.endObject();
        return builder.bytes();
    }

    public BytesReference convert(ESUpdateNode node) throws IOException {
        assert node != null;

        Context context = new Context();
        context.filteredFields.add("_version"); // will be handled by to UpdateFacet
        context.builder = XContentFactory.jsonBuilder().startObject();
        XContentBuilder builder = context.builder;

        whereClause(context, node.whereClause());

        if (node.version().isPresent()) {
            builder.field("version", true);
        }
        builder.startObject("facets").startObject(UpdateFacet.TYPE).startObject(UpdateFacet.TYPE);

        builder.field("doc", node.updateDoc());
        if (node.version().isPresent()) {
            builder.field("version", node.version().get());
        }

        builder.endObject().endObject().endObject();
        return builder.bytes();
    }

    private void addSorting(List<Symbol> orderBy,
                            boolean[] reverseFlags,
                            Boolean[] nullsFirst,
                            XContentBuilder builder) throws IOException {
        if (orderBy.isEmpty()) {
            return;
        }

        builder.startArray("sort");
        OrderByContext context = new OrderByContext(builder, reverseFlags, nullsFirst);
        for (Symbol symbol : orderBy) {
            orderByVisitor.process(symbol, context);
        }
        builder.endArray();
    }

    static class OrderByContext {
        final boolean[] reverseFlags;
        final Boolean[] nullsFirst;
        final XContentBuilder builder;
        int idx;

        public OrderByContext(XContentBuilder builder, boolean[] reverseFlags, Boolean[] nullsFirst) {
            this.builder = builder;
            this.reverseFlags = reverseFlags;
            this.nullsFirst = nullsFirst;
            this.idx = 0;
        }

        boolean reverseFlag() {
            return reverseFlags[idx];
        }

        @Nullable
        Boolean nullFirst() {
            return nullsFirst[idx];
        }
    }

    static class OrderBySymbolVisitor extends SymbolVisitor<OrderByContext, Void> {

        @Override
        protected Void visitSymbol(Symbol symbol, OrderByContext context) {
            throw new IllegalArgumentException(SymbolFormatter.format(
                    "Can't use \"%s\" in the ORDER BY clause", symbol));
        }

        @Override
        public Void visitReference(Reference symbol, OrderByContext context) {
            SortOrder sortOrder = new SortOrder(context.reverseFlag(), context.nullFirst());
            try {
                context.builder.startObject()
                        .startObject(symbol.info().ident().columnIdent().fqn())
                        .field("order", sortOrder.order())
                        .field("missing", sortOrder.missing())
                        .field("ignore_unmapped", true)
                        .endObject()
                        .endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            context.idx++;
            return null;
        }

        @Override
        public Void visitFunction(Function symbol, OrderByContext context) {
            if (symbol.info().ident().name().equals(DistanceFunction.NAME)) {
                Symbol referenceSymbol = symbol.arguments().get(0);
                Symbol valueSymbol = symbol.arguments().get(1);
                if (referenceSymbol.symbolType().isValueSymbol()) {
                    if (!valueSymbol.symbolType().isValueSymbol()) {
                        throw new IllegalArgumentException(SymbolFormatter.format(
                                "Can't use \"%s\" in the ORDER BY clause. Requires one column reference and one literal", symbol));
                    }
                    Symbol tmp = referenceSymbol;
                    referenceSymbol = valueSymbol;
                    valueSymbol = tmp;
                }

                SortOrder sortOrder = new SortOrder(context.reverseFlag(), context.nullFirst());
                Reference reference;
                Input input;
                try {
                    reference = (Reference) referenceSymbol;
                    input = (Input) valueSymbol;
                } catch (ClassCastException e) {
                    throw new IllegalArgumentException(SymbolFormatter.format(
                            "Can't use \"%s\" in the ORDER BY clause. Requires one column reference and one literal", symbol), e);
                }

                try {
                    context.builder.startObject().startObject("_geo_distance")
                            .field(reference.info().ident().columnIdent().fqn(), input.value())
                            .field("order", sortOrder.order())
                            .endObject()
                            .endObject();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                context.idx++;
                return null;
            } else {
                context.idx++;
                return visitSymbol(symbol, context);
            }
        }

        @Override
        public Void visitDynamicReference(DynamicReference symbol, OrderByContext context) {
            return visitReference(symbol, context);
        }

        private class SortOrder {
            private String order;
            private String missing;

            public SortOrder(boolean reverseFlag, Boolean nullFirst) {
                order = "asc";
                missing = "_last";
                if (reverseFlag) {
                    order = "desc";
                    missing = "_first";     // null > 'anyValue'; null values at the beginning.
                }
                if (nullFirst != null) {
                    missing = nullFirst ? "_first" : "_last";
                }
            }

            public String order() {
                return order;
            }

            public String missing() {
                return missing;
            }
        }
    }

    static class Context {
        XContentBuilder builder;
        Map<String, Object> ignoredFields = new HashMap<>();

        /**
         * these fields are ignored in the whereClause
         * (only applies to Function with 2 arguments and if left == reference and right == literal)
         */
        Set<String> filteredFields = new HashSet<String>(){{ add("_score"); }};

        /**
         * key = columnName
         * value = error message
         * <p/>
         * (in the _version case if the primary key is present a GetPlan is built from the planner and
         * the ESQueryBuilder is never used)
         */
        Map<String, String> unsupportedFields = ImmutableMap.<String, String>builder()
                .put("_version", "\"_version\" column is only valid in the WHERE clause if the primary key column is also present")
                .build();
    }

    static class Visitor extends SymbolVisitor<Context, Void> {

        private final ImmutableMap<String, Converter<? extends Symbol>> innerFunctions;
        private final ImmutableMap<String, Converter<? extends Symbol>> functions;

        Visitor() {
            EqConverter eqConverter = new EqConverter();
            RangeConverter ltConverter = new RangeConverter("lt");
            RangeConverter lteConverter = new RangeConverter("lte");
            RangeConverter gtConverter = new RangeConverter("gt");
            RangeConverter gteConverter = new RangeConverter("gte");
            functions = ImmutableMap.<String, Converter<? extends Symbol>>builder()
                    .put(AndOperator.NAME, new AndConverter())
                    .put(OrOperator.NAME, new OrConverter())
                    .put(EqOperator.NAME, eqConverter)
                    .put(LtOperator.NAME, ltConverter)
                    .put(LteOperator.NAME, lteConverter)
                    .put(GtOperator.NAME, gtConverter)
                    .put(GteOperator.NAME, gteConverter)
                    .put(LikeOperator.NAME, new LikeConverter())
                    .put(IsNullPredicate.NAME, new IsNullConverter())
                    .put(NotPredicate.NAME, new NotConverter())
                    .put(MatchFunction.NAME, new MatchConverter())
                    .put(InOperator.NAME, new InConverter())
                    .put(AnyEqOperator.NAME, eqConverter)
                    .put(AnyNeqOperator.NAME, new AnyNeqConverter())
                    .put(AnyLtOperator.NAME, ltConverter)
                    .put(AnyLteOperator.NAME, lteConverter)
                    .put(AnyGtOperator.NAME, gtConverter)
                    .put(AnyGteOperator.NAME, gteConverter)
                    .put(AnyLikeOperator.NAME, new LikeConverter())
                    .put(AnyNotLikeOperator.NAME, new AnyNotLikeConverter())
                    .build();


            innerFunctions = ImmutableMap.<String, Converter<? extends Symbol>>builder()
                    .put(DistanceFunction.NAME, new DistanceConverter())
                    .build();
        }

        static abstract class Converter<T extends Symbol> {

            /**
             * function that writes the xContent onto the context.
             * May abort early by returning false if it can't convert the argument.
             * Concrete implementation must not write to the context if it returns false.
             */
            public abstract boolean convert(T function, Context context) throws IOException;
        }


        class DistanceConverter extends Converter<Function> {

            private final Map<String, String> FIELD_NAME_MAP = ImmutableMap.<String, String>builder()
                    .put(GtOperator.NAME, "gt")
                    .put(GteOperator.NAME, "gte")
                    .put(LtOperator.NAME, "lt")
                    .put(LteOperator.NAME, "lte")
                    .build();

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                assert function.arguments().size() == 2;

                String functionName = function.info().ident().name();
                String valueFieldName = FIELD_NAME_MAP.get(functionName);

                context.builder.startObject("filtered");
                context.builder.startObject("query")
                        .startObject("match_all").endObject()
                        .endObject();
                context.builder.startObject("filter")
                        .startObject("geo_distance_range");

                Symbol valueSymbol;
                Symbol functionSymbol = function.arguments().get(0);
                if (functionSymbol.symbolType().isValueSymbol()) {
                    valueSymbol = functionSymbol;
                    functionSymbol = function.arguments().get(1);
                    if (functionSymbol.symbolType() != SymbolType.FUNCTION) {
                        throw new IllegalArgumentException("Can't compare two distance functions");
                    }
                } else {
                    valueSymbol = function.arguments().get(1);
                    if (!valueSymbol.symbolType().isValueSymbol()) {
                        throw new IllegalArgumentException("Can't compare two distance functions");
                    }
                }
                handleFunctionSymbol(context, (Function) functionSymbol);
                handleValueSymbol(context, functionName, valueFieldName, valueSymbol);

                context.builder.endObject(); // geo_distance_range
                context.builder.endObject(); // filter
                context.builder.endObject(); // filtered
                return true;
            }

            private void handleFunctionSymbol(Context context, Function functionSymbol) throws IOException {
                assert functionSymbol.arguments().size() == 2;
                assert functionSymbol.info().ident().name().equals(DistanceFunction.NAME);
                String fieldName = null;
                Double[] point = null;
                for (Symbol distanceArgument : functionSymbol.arguments()) {
                    if (distanceArgument instanceof Reference) {
                        fieldName = ((Reference)distanceArgument).info().ident().columnIdent().fqn();
                    } else if (distanceArgument.symbolType().isValueSymbol()) {
                        point = (Double[]) ((Input) distanceArgument).value();
                    }
                }
                assert fieldName != null;
                assert point != null;
                context.builder.field(fieldName, point);
            }

            private void handleValueSymbol(Context context,
                                           String functionName,
                                           String valueFieldName,
                                           Symbol valueSymbol) throws IOException {
                Literal literal = Literal.toLiteral(valueSymbol, DataTypes.DOUBLE);
                if (functionName.equals(EqOperator.NAME)) {
                    context.builder.field("from", literal.value());
                    context.builder.field("to", literal.value());
                    context.builder.field("include_upper", true);
                    context.builder.field("include_lower", true);
                } else {
                    context.builder.field(valueFieldName, literal.value());
                }
            }
        }

        class AndConverter extends Converter<Function> {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                context.builder.startObject("bool").startArray("must");

                for (Symbol symbol : function.arguments()) {
                    context.builder.startObject();
                    process(symbol, context);
                    context.builder.endObject();
                }
                context.builder.endArray().endObject();
                return true;
            }
        }

        class OrConverter extends Converter<Function> {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                context.builder.startObject("bool").field("minimum_should_match", 1).startArray("should");

                for (Symbol symbol : function.arguments()) {
                    context.builder.startObject();
                    process(symbol, context);
                    context.builder.endObject();
                }

                context.builder.endArray().endObject();
                return true;
            }
        }

        abstract class CmpConverter extends Converter<Function> {

            @Nullable
            protected Tuple<String, Object> prepare(Function function) {
                Preconditions.checkNotNull(function);
                Preconditions.checkArgument(function.arguments().size() == 2);

                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);

                if (left.symbolType() == SymbolType.FUNCTION || right.symbolType() == SymbolType.FUNCTION) {
                    return null;
                }

                assert left.symbolType() == SymbolType.REFERENCE || left.symbolType() == SymbolType.DYNAMIC_REFERENCE;


                Object value;
                if (Symbol.isLiteral(right, DataTypes.STRING)) {
                    Literal l = (Literal)right;
                    value = l.value();
                    if (value instanceof BytesRef) {
                        value = ((BytesRef)value).utf8ToString();
                    }
                } else {
                    assert right.symbolType().isValueSymbol();
                    value = ((Literal) right).value();
                }
                return new Tuple<>(((Reference) left).info().ident().columnIdent().fqn(), value);
            }
        }

        class EqConverter extends CmpConverter {
            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                if (tuple == null) {
                    return false;
                }
                context.builder.startObject("term").field(tuple.v1(), tuple.v2()).endObject();
                return true;
            }
        }

        class AnyNeqConverter extends CmpConverter {
            // 1 != ANY (col) --> gt 1 or lt 1
            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                if (tuple == null) {
                    return false;
                }
                context.builder.startObject("bool")
                                .field("minimum_should_match", 1)
                                .startArray("should")
                                    .startObject()
                                        .startObject("range")
                                            .startObject(tuple.v1())
                                                .field("lt", tuple.v2())
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                    .startObject()
                                        .startObject("range")
                                            .startObject(tuple.v1())
                                            .field("gt", tuple.v2())
                                            .endObject()
                                        .endObject()
                                    .endObject()
                                .endArray()
                            .endObject();
                return true;
            }
        }

        public static String convertWildcard(String wildcardString) {
            // lucene uses * and ? as wildcard characters
            // but via SQL they are used as % and _
            // here they are converted back.
            wildcardString = wildcardString.replaceAll("(?<!\\\\)\\*", "\\\\*");
            wildcardString = wildcardString.replaceAll("(?<!\\\\)%", "*");
            wildcardString = wildcardString.replaceAll("\\\\%", "%");

            wildcardString = wildcardString.replaceAll("(?<!\\\\)\\?", "\\\\?");
            wildcardString = wildcardString.replaceAll("(?<!\\\\)_", "?");
            return wildcardString.replaceAll("\\\\_", "_");
        }

        public static String convertWildcardToRegex(String wildcardString) {
            // lucene uses * and ? as wildcard characters
            // but via SQL they are used as % and _
            // here they are converted back.
            wildcardString = wildcardString.replaceAll("(?<!\\\\)\\*", "\\\\*");
            wildcardString = wildcardString.replaceAll("(?<!\\\\)%", ".*");
            wildcardString = wildcardString.replaceAll("\\\\%", "%");

            wildcardString = wildcardString.replaceAll("(?<!\\\\)\\?", "\\\\?");
            wildcardString = wildcardString.replaceAll("(?<!\\\\)_", ".");
            return wildcardString.replaceAll("\\\\_", "_");
        }



        class LikeConverter extends CmpConverter {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Tuple<String, Object> prepare = prepare(function);
                if (prepare == null) {
                    return false;
                }
                String like = prepare.v2().toString();
                like = convertWildcard(like);
                context.builder.startObject("wildcard").field(prepare.v1(), like).endObject();
                return true;
            }
        }

        class AnyNotLikeConverter extends LikeConverter {

            public String negateWildcard(String wildCard) {
                return String.format("~(%s)", wildCard);
            }

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Tuple<String, Object> prepare = prepare(function);
                if (prepare == null) {
                    return false;
                }
                String notLike = prepare.v2().toString();
                notLike = negateWildcard(convertWildcardToRegex(notLike));
                context.builder.startObject("regexp")
                            .startObject(prepare.v1())
                            .field("value", notLike)
                            .field("flags", "COMPLEMENT")
                            .endObject()
                            .endObject();
                return true;
            }
        }

        static class IsNullConverter extends Converter<Function> {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Preconditions.checkNotNull(function);
                Preconditions.checkArgument(function.arguments().size() == 1);

                Symbol arg = function.arguments().get(0);
                Preconditions.checkArgument(arg.symbolType() == SymbolType.REFERENCE);

                Reference reference = (Reference) arg;
                String columnName = reference.info().ident().columnIdent().fqn();

                context.builder.startObject("filtered").startObject("filter").startObject("missing")
                        .field("field", columnName)
                        .field("existence", true)
                        .field("null_value", true)
                        .endObject().endObject().endObject();
                return true;
            }
        }

        class NotConverter extends Converter<Function> {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Preconditions.checkNotNull(function);
                Preconditions.checkArgument(function.arguments().size() == 1);

                context.builder.startObject("bool").startObject("must_not");
                process(function.arguments().get(0), context);
                context.builder.endObject().endObject();
                return true;
            }
        }

        class RangeConverter extends CmpConverter {

            private final String operator;

            public RangeConverter(String operator) {
                this.operator = operator;
            }

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                if (tuple == null) {
                    return false;
                }
                context.builder.startObject("range")
                        .startObject(tuple.v1()).field(operator, tuple.v2()).endObject()
                        .endObject();
                return true;
            }
        }

        class MatchConverter extends CmpConverter {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                Tuple<String, Object> tuple = super.prepare(function);
                if (tuple == null) {
                    return false;
                }
                context.builder.startObject("match").field(tuple.v1(), tuple.v2()).endObject();
                return true;
            }
        }

        static class InConverter extends Converter<Function> {

            @Override
            public boolean convert(Function function, Context context) throws IOException {
                assert (function != null);
                assert (function.arguments().size() == 2);

                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);
                String refName = ((Reference) left).info().ident().columnIdent().fqn();
                Literal setLiteral = (Literal) right;
                boolean convertBytesRef = false;
                if (setLiteral.valueType().id() == SetType.ID
                        && ((SetType)setLiteral.valueType()).innerType().equals(DataTypes.STRING)) {
                    convertBytesRef = true;
                }
                context.builder.startObject("terms").field(refName);
                context.builder.startArray();
                for (Object o : (Set)setLiteral.value()) {
                    if (convertBytesRef) {
                        context.builder.value(((BytesRef) o).utf8ToString());
                    } else {
                        context.builder.value(o);
                    }
                }
                context.builder.endArray().endObject();
                return true;
            }

        }

        static class ReferenceConverter extends Converter<Reference> {

            public static final ReferenceConverter INSTANCE = new ReferenceConverter();

            private ReferenceConverter() {
            }

            protected Tuple<String, Boolean> prepare(Reference reference) {
                assert reference != null;
                return new Tuple<>(reference.info().ident().columnIdent().fqn(), true);
            }

            @Override
            public boolean convert(Reference reference, Context context) throws IOException {
                assert (reference != null);
                assert (reference.valueType() == DataTypes.BOOLEAN);
                Tuple<String, Boolean> tuple = prepare(reference);
                if (tuple == null) {
                    return false;
                }
                context.builder.startObject("term").field(tuple.v1(), tuple.v2()).endObject();
                return true;
            }
        }


        @Override
        public Void visitFunction(Function function, Context context) {
            assert function != null;

            try {
                if (fieldIgnored(function, context)) {
                    context.builder.field("match_all", new HashMap<>());
                    return null;
                }

                Converter converter = functions.get(function.info().ident().name());
                if (converter == null) {
                    return raiseUnsupported(function);
                }
                if (!convert(converter, function, context)) {
                    for (Symbol symbol : function.arguments()) {
                        if (symbol.symbolType() == SymbolType.FUNCTION) {
                            converter = innerFunctions.get(((Function) symbol).info().ident().name());
                            if (converter != null) {
                                convert(converter, function, context);
                                return null;
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            return null;
        }

        @SuppressWarnings("unchecked")
        private boolean convert(Converter converter, Function function, Context context) throws IOException {
            return converter.convert(function, context);
        }

        @Override
        public Void visitReference(Reference reference, Context context) {
            try {
                if (reference.valueType() == DataTypes.BOOLEAN) {
                    ReferenceConverter.INSTANCE.convert(reference, context);
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }

            return super.visitReference(reference, context);
        }

        private boolean fieldIgnored(Function function, Context context) {
            if (function.arguments().size() == 2) {
                Symbol left = function.arguments().get(0);
                Symbol right = function.arguments().get(1);

                if (left.symbolType() == SymbolType.REFERENCE && right.symbolType().isValueSymbol()) {
                    String columnName = ((Reference) left).info().ident().columnIdent().name();
                    if (context.filteredFields.contains(columnName)) {
                        context.ignoredFields.put(columnName, ((Literal) right).value());
                        return true;
                    }

                    String unsupported = context.unsupportedFields.get(columnName);
                    if (unsupported != null) {
                        throw new UnsupportedOperationException(unsupported);
                    }
                }
            }
            return false;
        }

        private Void raiseUnsupported(Symbol symbol) {
            throw new UnsupportedOperationException(
                    SymbolFormatter.format("Cannot convert function <%s> into a query", symbol));
        }
    }
}
