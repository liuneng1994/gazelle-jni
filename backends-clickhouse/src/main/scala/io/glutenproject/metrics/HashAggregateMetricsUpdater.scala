/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.glutenproject.metrics

import org.apache.spark.sql.execution.metric.SQLMetric

class HashAggregateMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {

  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      val aggregationParams = operatorMetrics.aggParams
      var currentIdx = operatorMetrics.metricsList.size() - 1
      var totalTime = 0L

      // read rel
      if (aggregationParams.isReadRel) {
        metrics("iterReadTime") +=
          (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
        metrics("outputRows") += operatorMetrics.metricsList.get(currentIdx).outputRows
        metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
        totalTime += operatorMetrics.metricsList.get(currentIdx).time
        currentIdx -= 1
      }

      // pre projection
      if (aggregationParams.preProjectionNeeded) {
        metrics("preProjectTime") +=
          (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
        metrics("outputRows") += operatorMetrics.metricsList.get(currentIdx).outputRows
        metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
        totalTime += operatorMetrics.metricsList.get(currentIdx).time
        currentIdx -= 1
      }

      // aggregating
      val aggMetricsData = operatorMetrics.metricsList.get(currentIdx)
      metrics("aggregatingTime") += (aggMetricsData.time / 1000L).toLong
      metrics("outputRows") += aggMetricsData.outputRows
      metrics("outputVectors") += aggMetricsData.outputVectors
      totalTime += aggMetricsData.time

      MetricsUtil.getAllProcessorList(aggMetricsData).foreach(processor => {
        if (!HashAggregateMetricsUpdater.INCLUDING_PROCESSORS.contains(processor.name)) {
          metrics("extraTime") += (processor.time / 1000L).toLong
        }
      })

      currentIdx -= 1

      // post projection
      if (aggregationParams.postProjectionNeeded) {
        metrics("postProjectTime") +=
          (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
        metrics("outputRows") += operatorMetrics.metricsList.get(currentIdx).outputRows
        metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
        totalTime += operatorMetrics.metricsList.get(currentIdx).time
        currentIdx -= 1
      }
      metrics("totalTime") += (totalTime / 1000L).toLong
    }
  }
}

object HashAggregateMetricsUpdater {

  val INCLUDING_PROCESSORS = Array(
    "AggregatingTransform",
    "MergingAggregatedTransform")
}
