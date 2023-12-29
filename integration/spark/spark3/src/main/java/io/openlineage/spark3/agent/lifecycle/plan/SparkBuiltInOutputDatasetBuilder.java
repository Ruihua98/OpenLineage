/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.BuiltInPlanUtils;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.nodes.OutputDatasetWithDelegate;
import io.openlineage.spark.builtin.nodes.OutputDatasetWithFacets;
import io.openlineage.spark.builtin.nodes.OutputDatasetWithIdentifier;
import io.openlineage.spark.builtin.nodes.OutputLineageNode;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/** {@link LogicalPlan} visitor that matches nodes which implement `OutputLineageNode` interface. */
@Slf4j
public class SparkBuiltInOutputDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<LogicalPlan> {

  public SparkBuiltInOutputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
    return logicalPlan instanceof OutputLineageNode;
  }

  @Override
  protected List<OpenLineage.OutputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    // Needs to cast to logical plan despite IntelliJ claiming otherwise.
    OutputLineageNode lineageNode = (OutputLineageNode) x;

    List<OutputDatasetWithFacets> datasets =
        ScalaConversionUtils.<OutputDatasetWithFacets>fromSeq(
            lineageNode.getOutputs(BuiltInPlanUtils.context(context)).seq());

    // extract datasets with delegate
    List<OutputDataset> outputDatasets =
        datasets.stream()
            .filter(d -> d instanceof OutputDatasetWithDelegate)
            .map(d -> (OutputDatasetWithDelegate) d)
            .map(outputDelegate -> (LogicalPlan) (outputDelegate.node()))
            .flatMap(d -> delegate(event, (LogicalPlan) d).stream())
            .collect(Collectors.toList());

    // extract datasets with identifier
    outputDatasets.addAll(
        datasets.stream()
            .filter(d -> d instanceof OutputDatasetWithIdentifier)
            .map(d -> (OutputDatasetWithIdentifier) d)
            .map(
                d ->
                    getContext()
                        .getOpenLineage()
                        .newOutputDatasetBuilder()
                        .namespace(d.datasetIdentifier().getNamespace())
                        .name(d.datasetIdentifier().getName())
                        .facets(d.facetsBuilder().build())
                        .outputFacets(d.outputFacetsBuilder().build())
                        .build())
            .collect(Collectors.toList()));

    return outputDatasets;
  }

  protected List<OutputDataset> delegate(SparkListenerEvent event, LogicalPlan plan) {
    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            plan,
            ScalaConversionUtils.toScalaFn(
                (lp) -> Collections.<OpenLineage.OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }

  /**
   * For testing purpose
   *
   * @return
   */
  protected OpenLineageContext getContext() {
    return context;
  }
}
