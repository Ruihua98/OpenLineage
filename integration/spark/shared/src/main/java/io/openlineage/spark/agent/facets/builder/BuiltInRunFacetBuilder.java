/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.util.BuiltInPlanUtils;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.nodes.FacetRegistry$;
import java.util.function.BiConsumer;

/** Builder to extract run facets from extensions via `FacetRegistry` static object. */
public class BuiltInRunFacetBuilder extends CustomFacetBuilder<Object, RunFacet> {
  private final OpenLineageContext openLineageContext;

  public BuiltInRunFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;

    FacetRegistry$.MODULE$.registerContext(BuiltInPlanUtils.context(openLineageContext));
  }

  @Override
  public boolean isDefinedAt(Object x) {
    return FacetRegistry$.MODULE$.runFacets() != null
        && !FacetRegistry$.MODULE$.runFacets().isEmpty();
  }

  @Override
  protected void build(Object event, BiConsumer<String, ? super RunFacet> consumer) {
    FacetRegistry$.MODULE$.runFacets().forEach((name, facet) -> consumer.accept(name, facet));
  }
}
