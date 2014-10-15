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

package co.cask.cdap.guides;

import co.cask.cdap.api.spark.AbstractSpark;
import co.cask.cdap.api.spark.Spark;
import co.cask.cdap.api.spark.SparkSpecification;
import co.cask.cdap.guides.PageRankProgram;

/**
 * Class for Spark program Specification
 */
public class PageRankProgramSpec extends AbstractSpark {
  /**
   * Configures a {@link Spark} job by returning a
   * {@link SparkSpecification}.
   *
   * @return An instance of {@link SparkSpecification}.
   */
  @Override
  public SparkSpecification configure() {
    return SparkSpecification.Builder.with()
      .setName("PageRankProgram")
      .setDescription("Spark program to compute PageRank")
      .setMainClassName(PageRankProgram.class.getName())
      .build();
  }
}
