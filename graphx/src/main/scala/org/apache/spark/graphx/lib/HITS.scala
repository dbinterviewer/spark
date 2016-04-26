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
 * https://en.wikipedia.org/wiki/HITS_algorithm
 *
 * The HITS algorithm generates a pair of scores for each node in the graph:
 * a hub score indicating the quality of the outgoing edges/links and
 * an authority score indicating the quality of the node/page itself.
 *
 * The algorithm consists of two updates.
 * 1) Authority update: The authority score at a node is updated to be the sum of all
 *    hub scores from incoming edges
 * 2) Hub update: The hub score is updated to be the sum of all authority scores on
 *    outgoing edges
 *
 * If A is the directed adjacency matrix for the graph and h, a are
 * the hub and authority scores, the updates can be expressed as
 * 1) a_new = h A
 * 2) h_new = a_new A^T
 * When the scores are normalized, it converges as the scores correspond to the
 * top left- and right-singular vectors of A.
 *
 * This implementation slightly differs from the original HITS algorithm in that
 * 1) The focused subgraph is assumed to be the input
 * 2) Nodes with 0 in- or 0 out-degree are not updated and, hence, keep their initial
 *    value up to the normalization constant. This primarily has consequences when
 *    the maximum degree of the graph is 1. Otherwise, the normalization of the graph
 *    will drive the score to 0.
 * 3) Instead of normalizing to have scores with L2 norm = 1,
 *    this uses L1 norm = 1
 */
object HITS extends Logging {
  private val MAX_ALLOWED_SCORE = 1e10

  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing  (Hub score, Authority score).
   * The scores are normalized to have L1 distance from 0 equal to 1.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
    * @param graph the base graph on which to compute HITS
   * @param numIter the number of iterations to run
    * @return the graph where each vertex contains the Hub and Authority scores
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), ED] =
  {
    require(numIter > 0, s"Number of iterations must be greater than 0," +
      s" but got ${numIter}")

    // Initialize the Hubs-Authority graph with each vertex containing
    // 1.0 for the Hub and Authority scores.
    var hubsAndAuthoritiesGraph: Graph[(Double, Double), ED] = graph
      // Set the vertex attributes to initial hub and authority scores
      .mapVertices { (id, attr) => (1.0, 1.0) }

    var iteration = 0
    var lastMaterializedGraph: Graph[(Double, Double), ED] = hubsAndAuthoritiesGraph
    val maxDegree = graph.degrees.values.max()
    var scoreBound = 1.0

    while (iteration < numIter) {
      // Compute the authority scores by summing the incoming hub scores
      val authorityUpdates: VertexRDD[Double] =
        hubsAndAuthoritiesGraph.aggregateMessages[Double](
          ctx => ctx.sendToDst(ctx.srcAttr._1), _ + _, TripletFields.Src
        )

      // update the authority scores
      hubsAndAuthoritiesGraph = hubsAndAuthoritiesGraph.joinVertices(authorityUpdates) {
        (id, oldScore, msgSum) => (oldScore._1, msgSum)
      }

      val hubUpdates = hubsAndAuthoritiesGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr._2), _ + _, TripletFields.Dst)

      // update the hub scores
      hubsAndAuthoritiesGraph = hubsAndAuthoritiesGraph.joinVertices(hubUpdates) {
          (id, oldScore, msgSum) => (msgSum, oldScore._2)
      }

      iteration += 1

      // A trivial bound on the max score is
      scoreBound *= maxDegree * maxDegree

      // Only materialize the VertexRDD and compute the normalization constant
      // if it is necessary to handle numerical precision issues
      if(scoreBound > MAX_ALLOWED_SCORE || iteration >= numIter) {
        val normalization = hubsAndAuthoritiesGraph.vertices.aggregate((0.0, 0.0))(
          (v, u) => (u._2._1 + v._1, u._2._2 + v._2),
          (v1, v2) => (v1._1 + v2._1, v1._2 + v2._2)
        )

        if (lastMaterializedGraph != null) {
          lastMaterializedGraph.vertices.unpersist(false)
          lastMaterializedGraph.edges.unpersist(false)
        }

        lastMaterializedGraph = hubsAndAuthoritiesGraph

        // Note that normalization constants can never be 0 unless the graph
        // is empty.
        hubsAndAuthoritiesGraph = hubsAndAuthoritiesGraph.mapVertices(
          (id, attr) => (attr._1 / normalization._1, attr._2 / normalization._2)
        ).cache()

        scoreBound = 1.0
      }

      logInfo(s"HITS finished iteration $iteration.")
    }

    hubsAndAuthoritiesGraph
  }

}
