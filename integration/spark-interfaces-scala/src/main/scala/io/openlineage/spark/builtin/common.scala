/** Copyright 2018-2023 contributors to the OpenLineage project
  * SPDX-License-Identifier: Apache-2.0
  */
package io.openlineage.spark.builtin.common

import io.openlineage.client.OpenLineage

trait OpenLineageContext {
  def openLineage: OpenLineage
}
