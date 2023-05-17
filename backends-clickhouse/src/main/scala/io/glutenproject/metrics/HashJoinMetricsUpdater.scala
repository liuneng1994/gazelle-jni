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

class HashJoinMetricsUpdater(val metrics: Map[String, SQLMetric]) extends MetricsUpdater {
  override def updateNativeMetrics(opMetrics: IOperatorMetrics): Unit = {
    if (opMetrics != null) {
      val operatorMetrics = opMetrics.asInstanceOf[OperatorMetrics]
      val joinParams = operatorMetrics.joinParams
      var currentIdx = operatorMetrics.metricsList.size() - 1
      var totalTime = 0L

      // stream side read rel
      if (joinParams.isStreamedReadRel) {
        metrics("streamIterReadTime") +=
          (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
        metrics("outputRows") += operatorMetrics.metricsList.get(currentIdx).outputRows
        metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
        totalTime += operatorMetrics.metricsList.get(currentIdx).time
        currentIdx -= 1
      }

      // stream side pre projection
      if (joinParams.streamPreProjectionNeeded) {
        metrics("streamPreProjectionTime") +=
          (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
        metrics("outputRows") += operatorMetrics.metricsList.get(currentIdx).outputRows
        metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
        totalTime += operatorMetrics.metricsList.get(currentIdx).time
        currentIdx -= 1
      }

      // build side read rel
      if (joinParams.isBuildReadRel) {
        val buildSideRealRel = operatorMetrics.metricsList.get(currentIdx)
        metrics("buildIterReadTime") += (buildSideRealRel.time / 1000L).toLong
        metrics("outputRows") += buildSideRealRel.outputRows
        metrics("outputVectors") += buildSideRealRel.outputVectors
        totalTime += buildSideRealRel.time

        // update fillingRightJoinSideTime
        MetricsUtil.getAllProcessorList(buildSideRealRel).foreach(processor => {
          if (processor.name.equalsIgnoreCase("FillingRightJoinSide")) {
            metrics("fillingRightJoinSideTime") += (processor.time / 1000L).toLong
          }
        })

        currentIdx -= 1
      }

      // build side pre projection
      if (joinParams.buildPreProjectionNeeded) {
        metrics("buildPreProjectionTime") +=
          (operatorMetrics.metricsList.get(currentIdx).time / 1000L).toLong
        metrics("outputRows") += operatorMetrics.metricsList.get(currentIdx).outputRows
        metrics("outputVectors") += operatorMetrics.metricsList.get(currentIdx).outputVectors
        totalTime += operatorMetrics.metricsList.get(currentIdx).time
        currentIdx -= 1
      }

      // joining
      val aggMetricsData = operatorMetrics.metricsList.get(currentIdx)
      metrics("probeTime") += (aggMetricsData.time / 1000L).toLong
      metrics("outputRows") += aggMetricsData.outputRows
      metrics("outputVectors") += aggMetricsData.outputVectors
      totalTime += aggMetricsData.time

      MetricsUtil.getAllProcessorList(aggMetricsData).foreach(processor => {
        if (!HashJoinMetricsUpdater.INCLUDING_PROCESSORS.contains(processor.name)) {
          metrics("extraTime") += (processor.time / 1000L).toLong
        }
      })

      currentIdx -= 1

      // post project
      if (joinParams.postProjectionNeeded) {
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

object HashJoinMetricsUpdater {
  val INCLUDING_PROCESSORS = Array(
    "JoiningTransform",
    "FillingRightJoinSideTransform")
}
