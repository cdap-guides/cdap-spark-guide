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

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.ObjectStores;
import co.cask.cdap.internal.io.UnsupportedTypeException;

/**
 * A CDAP PageRank application which computes the PageRank of connected URLs.
 */
public class PageRankApp extends AbstractApplication {

  public static final String RANKS_SERVICE = "PageRankService";
  public static final String BACKLINK_STREAM = "backlinkURLStream";
  public static final String RANKS_DATASET = "pageRanks";

  @Override
  public void configure() {
    setName(PageRankApp.class.getSimpleName());
    addSpark(new PageRankSpark());
    addStream(new Stream(BACKLINK_STREAM));
    addService(RANKS_SERVICE, new PageRankHandler());
    try {
      ObjectStores.createObjectStore(getConfigurer(), RANKS_DATASET, Double.class);
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("Will never happen: all classes above are supported", e);
    }
  }
}
