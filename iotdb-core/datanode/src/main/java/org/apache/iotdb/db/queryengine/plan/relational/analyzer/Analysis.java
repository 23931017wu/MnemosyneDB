/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.db.relational.sql.tree.AllColumns;
import org.apache.iotdb.db.relational.sql.tree.ExistsPredicate;
import org.apache.iotdb.db.relational.sql.tree.Expression;
import org.apache.iotdb.db.relational.sql.tree.FunctionCall;
import org.apache.iotdb.db.relational.sql.tree.InPredicate;
import org.apache.iotdb.db.relational.sql.tree.Join;
import org.apache.iotdb.db.relational.sql.tree.Node;
import org.apache.iotdb.db.relational.sql.tree.Offset;
import org.apache.iotdb.db.relational.sql.tree.OrderBy;
import org.apache.iotdb.db.relational.sql.tree.Parameter;
import org.apache.iotdb.db.relational.sql.tree.QuantifiedComparisonExpression;
import org.apache.iotdb.db.relational.sql.tree.Query;
import org.apache.iotdb.db.relational.sql.tree.QuerySpecification;
import org.apache.iotdb.db.relational.sql.tree.Relation;
import org.apache.iotdb.db.relational.sql.tree.Statement;
import org.apache.iotdb.db.relational.sql.tree.SubqueryExpression;
import org.apache.iotdb.db.relational.sql.tree.Table;
import org.apache.iotdb.tsfile.read.common.type.Type;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Streams;
import com.google.errorprone.annotations.Immutable;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.String.format;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class Analysis {

  @Nullable private final Statement root;

  private final Map<NodeRef<Parameter>, Expression> parameters;

  private String updateType;

  private final Map<NodeRef<Table>, Query> namedQueries = new LinkedHashMap<>();

  // map expandable query to the node being the inner recursive reference
  private final Map<NodeRef<Query>, Node> expandableNamedQueries = new LinkedHashMap<>();

  // map inner recursive reference in the expandable query to the recursion base scope
  private final Map<NodeRef<Node>, Scope> expandableBaseScopes = new LinkedHashMap<>();

  private final Map<NodeRef<Offset>, Long> offset = new LinkedHashMap<>();
  private final Map<NodeRef<Node>, OptionalLong> limit = new LinkedHashMap<>();
  private final Map<NodeRef<AllColumns>, List<Field>> selectAllResultFields = new LinkedHashMap<>();

  private final Map<NodeRef<Join>, Expression> joins = new LinkedHashMap<>();
  private final Map<NodeRef<Join>, JoinUsingAnalysis> joinUsing = new LinkedHashMap<>();
  private final Map<NodeRef<Node>, SubqueryAnalysis> subQueries = new LinkedHashMap<>();

  private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();

  private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
  private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();

  private final Map<NodeRef<Relation>, List<Type>> relationCoercions = new LinkedHashMap<>();

  private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();

  private final Map<NodeRef<Expression>, ResolvedField> columnReferences = new LinkedHashMap<>();

  private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates =
      new LinkedHashMap<>();
  private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, GroupingSetAnalysis> groupingSets =
      new LinkedHashMap<>();

  private final Map<NodeRef<Node>, Expression> where = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, Expression> having = new LinkedHashMap<>();
  private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();
  private final Set<NodeRef<OrderBy>> redundantOrderBy = new HashSet<>();
  private final Map<NodeRef<Node>, List<SelectExpression>> selectExpressions =
      new LinkedHashMap<>();

  private final Multimap<Field, SourceColumn> originColumnDetails = ArrayListMultimap.create();

  public Analysis(@Nullable Statement root, Map<NodeRef<Parameter>, Expression> parameters) {
    this.root = root;
    this.parameters = ImmutableMap.copyOf(requireNonNull(parameters, "parameters is null"));
  }

  public Map<NodeRef<Parameter>, Expression> getParameters() {
    return parameters;
  }

  public Statement getStatement() {
    return root;
  }

  public String getUpdateType() {
    return updateType;
  }

  public void setUpdateType(String updateType) {
    this.updateType = updateType;
  }

  public Query getNamedQuery(Table table) {
    return namedQueries.get(NodeRef.of(table));
  }

  public void registerNamedQuery(Table tableReference, Query query) {
    requireNonNull(tableReference, "tableReference is null");
    requireNonNull(query, "query is null");

    namedQueries.put(NodeRef.of(tableReference), query);
  }

  public void registerExpandableQuery(Query query, Node recursiveReference) {
    requireNonNull(query, "query is null");
    requireNonNull(recursiveReference, "recursiveReference is null");

    expandableNamedQueries.put(NodeRef.of(query), recursiveReference);
  }

  public boolean isExpandableQuery(Query query) {
    return expandableNamedQueries.containsKey(NodeRef.of(query));
  }

  public Node getRecursiveReference(Query query) {
    checkArgument(isExpandableQuery(query), "query is not registered as expandable");
    return expandableNamedQueries.get(NodeRef.of(query));
  }

  public void setExpandableBaseScope(Node node, Scope scope) {
    expandableBaseScopes.put(NodeRef.of(node), scope);
  }

  public Optional<Scope> getExpandableBaseScope(Node node) {
    return Optional.ofNullable(expandableBaseScopes.get(NodeRef.of(node)));
  }

  public Scope getScope(Node node) {
    return tryGetScope(node)
        .orElseThrow(
            () ->
                new IllegalArgumentException(
                    format("Analysis does not contain information for node: %s", node)));
  }

  public Optional<Scope> tryGetScope(Node node) {
    NodeRef<Node> key = NodeRef.of(node);
    if (scopes.containsKey(key)) {
      return Optional.of(scopes.get(key));
    }

    return Optional.empty();
  }

  public Scope getRootScope() {
    return getScope(root);
  }

  public void setScope(Node node, Scope scope) {
    scopes.put(NodeRef.of(node), scope);
  }

  public void addColumnReferences(Map<NodeRef<Expression>, ResolvedField> columnReferences) {
    this.columnReferences.putAll(columnReferences);
  }

  public Set<NodeRef<Expression>> getColumnReferences() {
    return unmodifiableSet(columnReferences.keySet());
  }

  public Map<NodeRef<Expression>, ResolvedField> getColumnReferenceFields() {
    return unmodifiableMap(columnReferences);
  }

  public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates) {
    this.aggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
  }

  public List<FunctionCall> getAggregates(QuerySpecification query) {
    return aggregates.get(NodeRef.of(query));
  }

  public void setOrderByAggregates(OrderBy node, List<Expression> aggregates) {
    this.orderByAggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
  }

  public List<Expression> getOrderByAggregates(OrderBy node) {
    return orderByAggregates.get(NodeRef.of(node));
  }

  public Map<NodeRef<Expression>, Type> getTypes() {
    return unmodifiableMap(types);
  }

  public Type getType(Expression expression) {
    Type type = types.get(NodeRef.of(expression));
    checkArgument(type != null, "Expression not analyzed: %s", expression);
    return type;
  }

  public void addType(Expression expression, Type type) {
    this.types.put(NodeRef.of(expression), type);
  }

  public void addTypes(Map<NodeRef<Expression>, Type> types) {
    this.types.putAll(types);
  }

  public List<Type> getRelationCoercion(Relation relation) {
    return relationCoercions.get(NodeRef.of(relation));
  }

  public void addRelationCoercion(Relation relation, Type[] types) {
    relationCoercions.put(NodeRef.of(relation), ImmutableList.copyOf(types));
  }

  public Map<NodeRef<Expression>, Type> getCoercions() {
    return unmodifiableMap(coercions);
  }

  public Type getCoercion(Expression expression) {
    return coercions.get(NodeRef.of(expression));
  }

  public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion) {
    this.coercions.put(NodeRef.of(expression), type);
    if (isTypeOnlyCoercion) {
      this.typeOnlyCoercions.add(NodeRef.of(expression));
    }
  }

  public void addCoercions(
      Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions) {
    this.coercions.putAll(coercions);
    this.typeOnlyCoercions.addAll(typeOnlyCoercions);
  }

  public void setGroupingSets(QuerySpecification node, GroupingSetAnalysis groupingSets) {
    this.groupingSets.put(NodeRef.of(node), groupingSets);
  }

  public boolean isAggregation(QuerySpecification node) {
    return groupingSets.containsKey(NodeRef.of(node));
  }

  public GroupingSetAnalysis getGroupingSets(QuerySpecification node) {
    return groupingSets.get(NodeRef.of(node));
  }

  public void setWhere(Node node, Expression expression) {
    where.put(NodeRef.of(node), expression);
  }

  public Expression getWhere(QuerySpecification node) {
    return where.get(NodeRef.<Node>of(node));
  }

  public void setOrderByExpressions(Node node, List<Expression> items) {
    orderByExpressions.put(NodeRef.of(node), ImmutableList.copyOf(items));
  }

  public List<Expression> getOrderByExpressions(Node node) {
    return orderByExpressions.get(NodeRef.of(node));
  }

  public void markRedundantOrderBy(OrderBy orderBy) {
    redundantOrderBy.add(NodeRef.of(orderBy));
  }

  public boolean isOrderByRedundant(OrderBy orderBy) {
    return redundantOrderBy.contains(NodeRef.of(orderBy));
  }

  public void setOffset(Offset node, long rowCount) {
    offset.put(NodeRef.of(node), rowCount);
  }

  public long getOffset(Offset node) {
    checkState(offset.containsKey(NodeRef.of(node)), "missing OFFSET value for node %s", node);
    return offset.get(NodeRef.of(node));
  }

  public void setLimit(Node node, OptionalLong rowCount) {
    limit.put(NodeRef.of(node), rowCount);
  }

  public void setLimit(Node node, long rowCount) {
    limit.put(NodeRef.of(node), OptionalLong.of(rowCount));
  }

  public OptionalLong getLimit(Node node) {
    checkState(limit.containsKey(NodeRef.of(node)), "missing LIMIT value for node %s", node);
    return limit.get(NodeRef.of(node));
  }

  public void setSelectAllResultFields(AllColumns node, List<Field> expressions) {
    selectAllResultFields.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
  }

  public List<Field> getSelectAllResultFields(AllColumns node) {
    return selectAllResultFields.get(NodeRef.of(node));
  }

  public void setSelectExpressions(Node node, List<SelectExpression> expressions) {
    selectExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
  }

  public List<SelectExpression> getSelectExpressions(Node node) {
    return selectExpressions.get(NodeRef.of(node));
  }

  public void setHaving(QuerySpecification node, Expression expression) {
    having.put(NodeRef.of(node), expression);
  }

  public Expression getHaving(QuerySpecification query) {
    return having.get(NodeRef.of(query));
  }

  public void setJoinUsing(Join node, JoinUsingAnalysis analysis) {
    joinUsing.put(NodeRef.of(node), analysis);
  }

  public JoinUsingAnalysis getJoinUsing(Join node) {
    return joinUsing.get(NodeRef.of(node));
  }

  public void setJoinCriteria(Join node, Expression criteria) {
    joins.put(NodeRef.of(node), criteria);
  }

  public Expression getJoinCriteria(Join join) {
    return joins.get(NodeRef.of(join));
  }

  public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis) {
    SubqueryAnalysis subqueries =
        this.subQueries.computeIfAbsent(NodeRef.of(node), key -> new SubqueryAnalysis());
    subqueries.addInPredicates(dereference(expressionAnalysis.getSubqueryInPredicates()));
    subqueries.addSubqueries(dereference(expressionAnalysis.getSubqueries()));
    subqueries.addExistsSubqueries(dereference(expressionAnalysis.getExistsSubqueries()));
    subqueries.addQuantifiedComparisons(dereference(expressionAnalysis.getQuantifiedComparisons()));
  }

  private <T extends Node> List<T> dereference(Collection<NodeRef<T>> nodeRefs) {
    return nodeRefs.stream().map(NodeRef::getNode).collect(toImmutableList());
  }

  public SubqueryAnalysis getSubqueries(Node node) {
    return subQueries.computeIfAbsent(NodeRef.of(node), key -> new SubqueryAnalysis());
  }

  public ResolvedField getResolvedField(Expression expression) {
    checkArgument(
        isColumnReference(expression), "Expression is not a column reference: %s", expression);
    return columnReferences.get(NodeRef.of(expression));
  }

  public boolean isColumnReference(Expression expression) {
    requireNonNull(expression, "expression is null");
    return columnReferences.containsKey(NodeRef.of(expression));
  }

  public RelationType getOutputDescriptor() {
    return getOutputDescriptor(root);
  }

  public RelationType getOutputDescriptor(Node node) {
    return getScope(node).getRelationType();
  }

  public void addSourceColumns(Field field, Set<SourceColumn> sourceColumn) {
    originColumnDetails.putAll(field, sourceColumn);
  }

  public Set<SourceColumn> getSourceColumns(Field field) {
    return ImmutableSet.copyOf(originColumnDetails.get(field));
  }

  public static class SourceColumn {
    private final QualifiedObjectName tableName;
    private final String columnName;

    public SourceColumn(QualifiedObjectName tableName, String columnName) {
      this.tableName = requireNonNull(tableName, "tableName is null");
      this.columnName = requireNonNull(columnName, "columnName is null");
    }

    public QualifiedObjectName getTableName() {
      return tableName;
    }

    public String getColumnName() {
      return columnName;
    }

    @Override
    public int hashCode() {
      return Objects.hash(tableName, columnName);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if ((obj == null) || (getClass() != obj.getClass())) {
        return false;
      }
      SourceColumn entry = (SourceColumn) obj;
      return Objects.equals(tableName, entry.tableName)
          && Objects.equals(columnName, entry.columnName);
    }

    @Override
    public String toString() {
      return toStringHelper(this)
          .add("tableName", tableName)
          .add("columnName", columnName)
          .toString();
    }
  }

  @Immutable
  public static final class SelectExpression {
    // expression refers to a select item, either to be returned directly, or unfolded by all-fields
    // reference
    // unfoldedExpressions applies to the latter case, and is a list of subscript expressions
    // referencing each field of the row.
    private final Expression expression;
    private final Optional<List<Expression>> unfoldedExpressions;

    public SelectExpression(Expression expression, Optional<List<Expression>> unfoldedExpressions) {
      this.expression = requireNonNull(expression, "expression is null");
      this.unfoldedExpressions = requireNonNull(unfoldedExpressions);
    }

    public Expression getExpression() {
      return expression;
    }

    public Optional<List<Expression>> getUnfoldedExpressions() {
      return unfoldedExpressions;
    }
  }

  public static final class JoinUsingAnalysis {
    private final List<Integer> leftJoinFields;
    private final List<Integer> rightJoinFields;
    private final List<Integer> otherLeftFields;
    private final List<Integer> otherRightFields;

    JoinUsingAnalysis(
        List<Integer> leftJoinFields,
        List<Integer> rightJoinFields,
        List<Integer> otherLeftFields,
        List<Integer> otherRightFields) {
      this.leftJoinFields = ImmutableList.copyOf(leftJoinFields);
      this.rightJoinFields = ImmutableList.copyOf(rightJoinFields);
      this.otherLeftFields = ImmutableList.copyOf(otherLeftFields);
      this.otherRightFields = ImmutableList.copyOf(otherRightFields);

      checkArgument(
          leftJoinFields.size() == rightJoinFields.size(),
          "Expected join fields for left and right to have the same size");
    }

    public List<Integer> getLeftJoinFields() {
      return leftJoinFields;
    }

    public List<Integer> getRightJoinFields() {
      return rightJoinFields;
    }

    public List<Integer> getOtherLeftFields() {
      return otherLeftFields;
    }

    public List<Integer> getOtherRightFields() {
      return otherRightFields;
    }
  }

  public static class GroupingSetAnalysis {
    private final List<Expression> originalExpressions;

    private final List<List<Set<FieldId>>> cubes;
    private final List<List<Set<FieldId>>> rollups;
    private final List<List<Set<FieldId>>> ordinarySets;
    private final List<Expression> complexExpressions;

    public GroupingSetAnalysis(
        List<Expression> originalExpressions,
        List<List<Set<FieldId>>> cubes,
        List<List<Set<FieldId>>> rollups,
        List<List<Set<FieldId>>> ordinarySets,
        List<Expression> complexExpressions) {
      this.originalExpressions = ImmutableList.copyOf(originalExpressions);
      this.cubes = ImmutableList.copyOf(cubes);
      this.rollups = ImmutableList.copyOf(rollups);
      this.ordinarySets = ImmutableList.copyOf(ordinarySets);
      this.complexExpressions = ImmutableList.copyOf(complexExpressions);
    }

    public List<Expression> getOriginalExpressions() {
      return originalExpressions;
    }

    public List<List<Set<FieldId>>> getCubes() {
      return cubes;
    }

    public List<List<Set<FieldId>>> getRollups() {
      return rollups;
    }

    public List<List<Set<FieldId>>> getOrdinarySets() {
      return ordinarySets;
    }

    public List<Expression> getComplexExpressions() {
      return complexExpressions;
    }

    public Set<FieldId> getAllFields() {
      return Streams.concat(
              cubes.stream().flatMap(Collection::stream).flatMap(Collection::stream),
              rollups.stream().flatMap(Collection::stream).flatMap(Collection::stream),
              ordinarySets.stream().flatMap(Collection::stream).flatMap(Collection::stream))
          .collect(toImmutableSet());
    }
  }

  public static class SubqueryAnalysis {
    private final List<InPredicate> inPredicatesSubqueries = new ArrayList<>();
    private final List<SubqueryExpression> subqueries = new ArrayList<>();
    private final List<ExistsPredicate> existsSubqueries = new ArrayList<>();
    private final List<QuantifiedComparisonExpression> quantifiedComparisonSubqueries =
        new ArrayList<>();

    public void addInPredicates(List<InPredicate> expressions) {
      inPredicatesSubqueries.addAll(expressions);
    }

    public void addSubqueries(List<SubqueryExpression> expressions) {
      subqueries.addAll(expressions);
    }

    public void addExistsSubqueries(List<ExistsPredicate> expressions) {
      existsSubqueries.addAll(expressions);
    }

    public void addQuantifiedComparisons(List<QuantifiedComparisonExpression> expressions) {
      quantifiedComparisonSubqueries.addAll(expressions);
    }

    public List<InPredicate> getInPredicatesSubqueries() {
      return unmodifiableList(inPredicatesSubqueries);
    }

    public List<SubqueryExpression> getSubqueries() {
      return unmodifiableList(subqueries);
    }

    public List<ExistsPredicate> getExistsSubqueries() {
      return unmodifiableList(existsSubqueries);
    }

    public List<QuantifiedComparisonExpression> getQuantifiedComparisonSubqueries() {
      return unmodifiableList(quantifiedComparisonSubqueries);
    }
  }
}
