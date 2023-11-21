/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.spark.agent.util.BuiltInPlanUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.nodes.FacetRegistry$;
import java.util.function.BiConsumer;

/** Builder to extract job facets from extensions via `FacetRegistry` static object. */
public class BuiltInJobFacetBuilder extends CustomFacetBuilder<Object, JobFacet> {

  private final OpenLineageContext openLineageContext;

  public BuiltInJobFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;

    FacetRegistry$.MODULE$.registerContext(BuiltInPlanUtils.context(openLineageContext));
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return FacetRegistry$.MODULE$.jobFacets() != null
        && !FacetRegistry$.MODULE$.jobFacets().isEmpty();
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super JobFacet> consumer) {
    FacetRegistry$.MODULE$.jobFacets().forEach((name, facet) -> consumer.accept(name, facet));
  }
}
