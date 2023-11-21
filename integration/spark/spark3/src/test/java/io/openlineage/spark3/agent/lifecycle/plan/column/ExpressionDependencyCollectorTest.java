/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.column;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.column.ColumnLevelLineageNode;
import io.openlineage.spark.builtin.column.ExpressionDependency;
import io.openlineage.spark.builtin.column.ExpressionDependencyWithDelegate;
import io.openlineage.spark.builtin.column.ExpressionDependencyWithIdentifier;
import io.openlineage.spark.builtin.column.OlExprId;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.BinaryExpression;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression;
import org.apache.spark.sql.catalyst.plans.logical.Aggregate;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata$;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.Seq$;

class ExpressionDependencyCollectorTest {

  ColumnLevelLineageBuilder builder = Mockito.mock(ColumnLevelLineageBuilder.class);
  OpenLineageContext context = mock(OpenLineageContext.class);

  ExprId exprId1 = mock(ExprId.class);
  ExprId exprId2 = mock(ExprId.class);

  ExprId aliasExprId1 = mock(ExprId.class);
  ExprId aliasExprId2 = mock(ExprId.class);

  NamedExpression expression1 =
      new AttributeReference(
          "name1", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId1, null);
  NamedExpression expression2 =
      new AttributeReference(
          "name2", IntegerType$.MODULE$, false, Metadata$.MODULE$.empty(), exprId2, null);

  Alias alias1 =
      new Alias(
          (Expression) expression1,
          "name1",
          aliasExprId1,
          (Seq<String>) Seq$.MODULE$.empty(),
          Option.empty(),
          (Seq<String>) Seq$.MODULE$.empty());

  Alias alias2 =
      new Alias(
          (Expression) expression2,
          "name2",
          aliasExprId2,
          (Seq<String>) Seq$.MODULE$.empty(),
          Option.empty(),
          (Seq<String>) Seq$.MODULE$.empty());

  @Test
  void testCollectFromProjectPlan() {
    Project project =
        new Project(
            toScalaSeq(Arrays.asList((NamedExpression) alias1, (NamedExpression) alias2)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan, builder);

    verify(builder, times(1)).addDependency(aliasExprId1, exprId1);
    verify(builder, times(1)).addDependency(aliasExprId2, exprId2);
  }

  @Test
  void testCollectFromAggregatePlan() {
    Aggregate aggregate =
        new Aggregate(
            (Seq<Expression>) Seq$.MODULE$.empty(),
            toScalaSeq(Arrays.asList((NamedExpression) alias1)),
            mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, aggregate, null, null, false);

    ExpressionDependencyCollector.collect(context, plan, builder);

    verify(builder, times(1)).addDependency(aliasExprId1, exprId1);
  }

  @Test
  void testCollectTraversingExpressions() {
    // AggregateExpression
    AggregateExpression aggr1 = mock(AggregateExpression.class);
    AggregateExpression aggr2 = mock(AggregateExpression.class);

    when(aggr1.resultId()).thenReturn(exprId1);
    when(aggr2.resultId()).thenReturn(exprId2);

    // BinaryExpression
    Seq<Expression> children =
        scala.collection.JavaConverters.collectionAsScalaIterableConverter(
                Arrays.asList((Expression) aggr1, aggr2))
            .asScala()
            .toSeq();
    BinaryExpression binaryExpression = mock(BinaryExpression.class);
    when(binaryExpression.children()).thenReturn(children);

    ExprId rootAliasExprId = mock(ExprId.class);
    Alias rootAlias =
        new Alias(
            (Expression) binaryExpression,
            "name2",
            rootAliasExprId,
            (Seq<String>) Seq$.MODULE$.empty(),
            Option.empty(),
            (Seq<String>) Seq$.MODULE$.empty());

    Project project = new Project(toScalaSeq(Arrays.asList(rootAlias)), mock(LogicalPlan.class));
    LogicalPlan plan = new CreateTableAsSelect(null, null, null, project, null, null, false);

    ExpressionDependencyCollector.collect(context, plan, builder);

    verify(builder, times(1)).addDependency(rootAliasExprId, exprId1);
    verify(builder, times(1)).addDependency(rootAliasExprId, exprId2);
  }

  @Test
  void testBuiltInColumnLevelLineageWithIdentifier() {
    LogicalPlan columnLineagePlanNode =
        mock(LogicalPlan.class, withSettings().extraInterfaces(ColumnLevelLineageNode.class));

    OlExprId outputExprId = new OlExprId(1L);
    OlExprId inputExprId1 = new OlExprId(2L);
    OlExprId inputExprId2 = new OlExprId(3L);

    when(((ColumnLevelLineageNode) columnLineagePlanNode).columnLevelLineageDependencies(any()))
        .thenReturn(
            JavaConverters.asScalaBuffer(
                    Collections.<ExpressionDependency>singletonList(
                        new ExpressionDependencyWithIdentifier(
                            outputExprId,
                            JavaConverters.asScalaBuffer(Arrays.asList(inputExprId1, inputExprId2))
                                .toList())))
                .toList());

    ExpressionDependencyCollector.collectFromNode(context, columnLineagePlanNode, builder);

    verify(builder, times(1)).addDependency(ExprId.apply(1L), ExprId.apply(2L));
    verify(builder, times(1)).addDependency(ExprId.apply(1L), ExprId.apply(3L));
  }

  @Test
  void testBuiltInColumnLevelLineageWithDelegate() {
    LogicalPlan columnLineagePlanNode =
        mock(LogicalPlan.class, withSettings().extraInterfaces(ColumnLevelLineageNode.class));

    when(((ColumnLevelLineageNode) columnLineagePlanNode).columnLevelLineageDependencies(any()))
        .thenReturn(
            JavaConverters.asScalaBuffer(
                    Collections.<ExpressionDependency>singletonList(
                        new ExpressionDependencyWithDelegate(
                            new OlExprId(1L),
                            new AttributeReference(
                                "name1",
                                IntegerType$.MODULE$,
                                false,
                                Metadata$.MODULE$.empty(),
                                ExprId.apply(2L),
                                null))))
                .toList());

    ExpressionDependencyCollector.collectFromNode(context, columnLineagePlanNode, builder);

    verify(builder, times(1)).addDependency(ExprId.apply(1L), ExprId.apply(2L));
  }

  private Seq<NamedExpression> toScalaSeq(Collection<NamedExpression> expressions) {
    return scala.collection.JavaConverters.collectionAsScalaIterableConverter(expressions)
        .asScala()
        .toSeq();
  }
}
