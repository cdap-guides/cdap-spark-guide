/*
 * Copyright © 2014 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.guides

import co.cask.cdap.api.common.Bytes
import co.cask.cdap.api.spark.{ScalaSparkProgram, SparkContext}
import org.apache.hadoop.io.Text
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * Spark program to compute PageRanks.
 */
class PageRankProgram extends ScalaSparkProgram {

  private final val ITERATIONS_COUNT: Int = 10

  override def run(sc: SparkContext) {
    val lines: RDD[(Array[Byte], Text)] = sc.readFromStream("backlinkURLStream", classOf[Text])
    val links = lines.map { s =>
      val parts = s._2.toString.split("\\s+")
      (parts(0), parts(1))
    }.distinct().groupByKey().cache()

    var ranks = links.mapValues(v => 1.0)

    // Calculate the PageRanks
    for (i <- 1 to ITERATIONS_COUNT) {
      val contribs = links.join(ranks).values.flatMap { case (urls, rank) =>
        val size = urls.size
        urls.map(url => (url, rank / size))
      }
      ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)
    }

    val output = ranks.map(x => (Bytes.toBytes(x._1), x._2))

    sc.writeToDataset(output, "pageRanks", classOf[Array[Byte]], classOf[java.lang.Double])
  }
}
