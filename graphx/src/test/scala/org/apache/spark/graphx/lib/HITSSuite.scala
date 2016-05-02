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

import org.apache.spark.SparkFunSuite
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators

class HITSSuite extends SparkFunSuite with LocalSparkContext {

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val numIter = 3
      val tol = 1e-3
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()

      // Normally this should converge in 1 iteration to the stationary point
      // However, due to the initialization of scores to 1.0 and that
      // nodes with 0 in- or out- degree do not have their authority or hub scores
      // updated, it only asymptotically reaches the stationary point
      val hubAndAuthorityGraph = starGraph.runHITS(numIter)

      val scores = hubAndAuthorityGraph.vertices.collect()
      // for better readability I should just collect as map directly on the degrees
      // and do the translation to score in the loop itself
      val outDegrees = starGraph.outDegrees.collectAsMap()
      val isStarNode = (v: VertexId) => (outDegrees.getOrElse(v, 0.0) == 0.0)

      for ((vid, (hubScore, authorityScore)) <- scores) {
        val targetHubScore = if (isStarNode(vid)) 0.0 else 1.0 / (nVertices.toDouble - 1)
        val targetAuthorityScore = if (isStarNode(vid)) 1.0 else 0.0

        assert( Math.abs(targetHubScore - hubScore) < tol )
        assert( Math.abs(targetAuthorityScore - authorityScore) < tol )
      }
    }
  } // end of test Star HITS

  test("Grid HITS") {
    withSpark { sc =>
      val tol = 2e-2
      val rows = 5
      val cols = 5
      val numIter = 30

      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()
      val hubsAndAuthoritiesGraph = gridGraph.runHITS(numIter)

      // Manually verified hub scores by constructing a dense adjacency matrix
      // and performing power iterations
      // In R where A is the directed adjacency matrix
      // for(i in 1:30) {v = v %*% A %*% t(A); v = v / sum(v) }
      val targetHubScores = Array(
        0.000000012,
        0.000681770,
        0.019118022,
        0.077558213,
        0.042873126,
        0.000681770,
        0.027036965,
        0.125491825,
        0.112243300,
        0.011199078,
        0.019118022,
        0.125491825,
        0.138740349,
        0.027036965,
        0.000454513,
        0.077558213,
        0.112243300,
        0.027036965,
        0.000909026,
        0.000000012,
        0.042873126,
        0.011199078,
        0.000454513,
        0.000000012,
        0.000000000
        )

      val scores = hubsAndAuthoritiesGraph.vertices.collect()
      for ((vid, (hubScore, authorityScore)) <- scores) {
        assert( Math.abs(targetHubScores(vid.toInt) - hubScore) < tol )
      }
    }
  } // end of Grid HITS

  test("Chain HITS") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val numIter = 2
      val tol = 1e-3

      val hubsAndAuthoritiesGraph = HITS.run(chain, numIter)

      val scores = hubsAndAuthoritiesGraph.vertices.collect()
      val outDegrees = chain.outDegrees.collectAsMap()
      val inDegrees = chain.inDegrees.collectAsMap()
      val isChainStart = (v: VertexId) => inDegrees.getOrElse(v, 0) == 0
      val isChainEnd = (v: VertexId) => outDegrees.getOrElse(v, 0) == 0

      for ((vid, (hubScore, authorityScore)) <- scores) {
        val targetHubScore = if (isChainEnd(vid)) 0.0 else 1.0 / 9.0
        val targetAuthorityScore = if (isChainStart(vid)) 0.0 else 1.0 / 9.0

        assert( Math.abs(targetHubScore - hubScore) < tol )
        assert( Math.abs(targetAuthorityScore - authorityScore) < tol )
      }
    }
  } // end of Chain HITS

}
