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

package org.apache.spark.graphx.lib

import scala.language.postfixOps
import scala.reflect.ClassTag

import org.apache.spark.graphx._
import org.apache.spark.internal.Logging

/**
 * HITS algorithm implementation.
 * http://www.cs.cornell.edu/home/kleinber/auth.pdf
 * Instead of normalizing to have scores with L2 norm = 1,
 * this uses L1 norm = 1 
 */
object HITS extends Logging {
  val MAX_ALLOWED_SCORE = 1e10

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing  (Hub score, Authority score).
   * The scores are normalized to have L1 distance from 0 = |V|.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the base graph on which to compute HITS
   * @param numIter the number of iterations to run
   *
   * @return the graph where each vertex contains the Hub and Authority scores
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Unit.type] =
  {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")

    // Initialize the Hubs-Authority graph with each vertex containing
    // 1.0 for the Hub and Authority scores.
    var hubsAndAuthoritiesGraph: Graph[(Double, Double), Unit.type] = graph
      // Set the vertex attributes to initial hub and authority scores
      .mapTriplets( e => Unit )
      .mapVertices { (id, attr) => (1.0, 1.0)
      }

    var iteration = 0
    var prevGraph: Graph[(Double, Double), Unit.type] = null
    val maxDegree = graph.degrees.map { case (_, deg) => deg }.max()
    var normalization: (Double, Double) = null
    var scoreBound = 1.0

    while (iteration < numIter) {
      hubsAndAuthoritiesGraph.cache()

      // Compute the authority scores by summing the incoming hub scores
      val authorityUpdates: VertexRDD[Double] = hubsAndAuthoritiesGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _, TripletFields.Src)

      prevGraph = hubsAndAuthoritiesGraph

      // update the authority scores
      hubsAndAuthoritiesGraph = hubsAndAuthoritiesGraph.joinVertices(authorityUpdates) {
        (id, oldScore, msgSum) => (oldScore._1, msgSum)
      }.cache()

      val hubUpdates = hubsAndAuthoritiesGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._2), _ + _, TripletFields.Dst)

      // update the hub scores
      hubsAndAuthoritiesGraph = hubsAndAuthoritiesGraph.joinVertices(hubUpdates) {
          (id, oldScore, msgSum) => (msgSum, oldScore._2)
      }.cache()

      logInfo(s"HITS finished iteration $iteration.")
      prevGraph.vertices.unpersist(false)
      prevGraph.edges.unpersist(false)

      iteration += 1
      scoreBound *= maxDegree * maxDegree

      if(scoreBound > MAX_ALLOWED_SCORE || iteration >= numIter) {
        normalization = hubsAndAuthoritiesGraph.vertices.aggregate((0.0, 0.0))(
          (v, u) => (u._2._1 + v._1, u._2._2 + v._2),
          (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)
        )
        hubsAndAuthoritiesGraph = hubsAndAuthoritiesGraph.mapVertices(
          (id, attr) => (attr._1 / normalization._1, attr._2 / normalization._2)
        )
        scoreBound = 1.0
      }
    }

    hubsAndAuthoritiesGraph
  }

}
