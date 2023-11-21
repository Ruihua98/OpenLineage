/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.builtin.nodes.FacetRegistry$;
import java.util.function.BiConsumer;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class BuiltInRunFacetBuilderTest {

  OpenLineageContext context = mock(OpenLineageContext.class, RETURNS_DEEP_STUBS);
  BuiltInRunFacetBuilder builder = new BuiltInRunFacetBuilder(context);

  @BeforeEach
  void setup() {
    FacetRegistry$.MODULE$.clear();
  }

  @Test
  void testIsDefinedWhenNoFacetsRegistered() {
    assertThat(builder.isDefinedAt(mock(LogicalPlan.class))).isFalse();
  }

  @Test
  void testIsDefined() {
    FacetRegistry$.MODULE$.registerRunFacet("some-facet", mock(RunFacet.class));
    assertThat(builder.isDefinedAt(mock(LogicalPlan.class))).isTrue();
  }

  @Test
  void testApply() {
    BiConsumer<String, ? super RunFacet> consumer = mock(BiConsumer.class);

    RunFacet facet1 = mock(RunFacet.class);
    RunFacet facet2 = mock(RunFacet.class);

    FacetRegistry$.MODULE$.registerRunFacet("facet1", facet1);
    FacetRegistry$.MODULE$.registerRunFacet("facet2", facet2);

    builder.build(mock(LogicalPlan.class), consumer);

    verify(consumer, times(1)).accept("facet1", facet1);
    verify(consumer, times(1)).accept("facet2", facet2);
  }
}
