/** Copyright 2018-2023 contributors to the OpenLineage project
  * SPDX-License-Identifier: Apache-2.0
  */
package io.openlineage.spark.builtin.nodes

import io.openlineage.client.OpenLineage.RunFacet
import io.openlineage.client.OpenLineage.JobFacet
import io.openlineage.spark.builtin.common.OpenLineageContext

import java.util

/** Companion object to store run and job facets to be included in the next sent
  * event. Facets are lazily emitted next Openlineage event is emitted.
  */
object FacetRegistry {
  // Java maps are used to avoid Scala version mismatch
  var runFacets: java.util.Map[String, RunFacet] = new util.HashMap()
  var jobFacets: java.util.Map[String, JobFacet] = new util.HashMap()
  var openlineageContext: Option[OpenLineageContext] = Option.empty;

  def registerContext(openLineageContext: OpenLineageContext): Unit = {
    FacetRegistry.openlineageContext = Option.apply(openLineageContext);
  }

  def registerRunFacet(name: String, facet: RunFacet): Unit = {
    runFacets.put(name, facet)
  }

  def registerJobFacet(name: String, facet: JobFacet): Unit = {
    jobFacets.put(name, facet)
  }

  def clear(): Unit = {
    runFacets.clear()
    jobFacets.clear()
  }
}
