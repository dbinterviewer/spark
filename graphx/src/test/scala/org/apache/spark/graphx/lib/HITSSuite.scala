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

      val hubAndAuthorityGraph = HITS.run(starGraph, 10)
      val targetHubScores = starGraph.outDegrees.mapValues(
        v => if (v == 1.0) 1.0 / (nVertices.toDouble - 1) else 0.0
      )
      val targetAuthorityScores = starGraph.inDegrees.mapValues(
        v => if (v == nVertices - 1) 1.0 else 0.0
      )

      //targetHubScores.collect().foreach(println)
//      println("---------")
//      hubAndAuthorityGraph.vertices.collect.foreach(println)

      val nonMatchingHubScores = targetHubScores.innerJoin[(Double, Double), Long](
        hubAndAuthorityGraph.vertices) {
        (vid, target, estimated) =>
          if (Math.abs(target - estimated._1) > tol) 1 else 0
      }.map { case (vid, notMatch) => notMatch }.sum()
      assert(nonMatchingHubScores === 0)

      val nonMatchingAuthorityScores = targetAuthorityScores.innerJoin[(Double, Double), Long](
        hubAndAuthorityGraph.vertices) {
        (vid, target, estimated) =>
          if (Math.abs(target - estimated._2) > tol) 1 else 0
      }.map { case (vid, notMatch) => notMatch }.sum()
      assert(nonMatchingAuthorityScores === 0)
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

      // manually verified hub scores
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
      val numIter = 4
      val tol = 1e-3

      val hubsAndAuthoritiesGraph = HITS.run(chain, numIter)

      // all scores should remain = 1
      val nonMatchingScores = hubsAndAuthoritiesGraph.vertices.map {
        case (vid, attr) =>
          if ( Math.abs(attr._1 - 0.1) < tol && Math.abs(attr._2 - 0.1) < tol) 0 else 1
      }.sum()

      hubsAndAuthoritiesGraph.vertices.map {
        case (vid, attr) =>
          if ( Math.abs(attr._1 - 0.1) < tol && Math.abs(attr._2 - 0.1) < tol) 0 else 1
      }.collect().foreach(println)
      assert(nonMatchingScores === 0)
    }
  }

}
