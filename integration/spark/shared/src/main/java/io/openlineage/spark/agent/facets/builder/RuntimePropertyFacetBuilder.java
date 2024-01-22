/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;
import io.openlineage.spark.agent.facets.RuntimePropertyFacet;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.function.BiConsumer;

public class RuntimePropertyFacetBuilder
        extends CustomFacetBuilder<SparkListenerJobEnd, RuntimePropertyFacet> {

    public RuntimePropertyFacetBuilder(){}

    @Override
    protected void build(SparkListenerJobEnd event, BiConsumer<String, ? super RuntimePropertyFacet> consumer) {
        consumer.accept("spark_properties", new RuntimePropertyFacet());
    }
}
