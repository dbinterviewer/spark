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
      val tol = 1e-3
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()

      // Normally this should converge in 1 iteration to the stationary point
      // However, due to the initialization of scores to 1.0 and that
      // nodes with 0 in- or out- degree do not have their authority or hub scores
      // updated, it only asymptotically reaches the stationary point
      val hubAndAuthorityGraph = starGraph.runHITS(3)

      val scores = hubAndAuthorityGraph.vertices.collect()
      // for better readability I should just collect as map directly on the degrees
      // and do the translation to score in the loop itself
      val targetHubScores = starGraph.outDegrees.mapValues(
        v => if (v == 1.0) 1.0 / (nVertices.toDouble - 1) else 0.0
      ).collectAsMap()
      val targetAuthorityScores = starGraph.inDegrees.mapValues(
        v => if (v == nVertices - 1) 1.0 else 0.0
      ).collectAsMap()

      for ((vid, (hubScore, authorityScore)) <- scores) {
        assert( Math.abs(targetHubScores.getOrElse(vid, 0.0) - hubScore) < tol )
        assert( Math.abs(targetAuthorityScores.getOrElse(vid, 0.0) - authorityScore) < tol )
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
      val hubsAndAuthoritiesGraph = HITS.run(gridGraph, numIter)

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

      val errors = hubsAndAuthoritiesGraph.vertices.map { case (vid, attr) =>
        if (Math.abs(targetHubScores(vid.toInt) - attr._1) > tol) 1 else 0
      }.sum()

      assert(errors === 0)
    }
  } // end of Grid HITS

  test("Chain HITS") {
    withSpark { sc =>
      val chain1 = (0 until 9).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      // The graph is initialized at a stationary point (up to normalization)
      // so the number of iterations does not matter for this test
      val numIter = 4
      val tol = 1e-3

      val hubsAndAuthoritiesGraph = HITS.run(chain, numIter)

      // all scores should remain = 1
      val nonMatchingScores = hubsAndAuthoritiesGraph.vertices.map {
        case (vid, attr) =>
          if ( Math.abs(attr._1 - 0.1) < tol && Math.abs(attr._2 - 0.1) < tol) 0 else 1
      }.sum()

      assert(nonMatchingScores === 0)
    }
  } // end of Chain HITS

}
