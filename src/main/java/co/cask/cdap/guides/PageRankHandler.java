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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.ObjectStore;
import co.cask.cdap.api.service.http.AbstractHttpServiceHandler;
import co.cask.cdap.api.service.http.HttpServiceRequest;
import co.cask.cdap.api.service.http.HttpServiceResponder;
import com.google.common.base.Charsets;

import java.net.HttpURLConnection;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

/**
 * Handles queries retrieving the PageRank of a URL.
 */
public class PageRankHandler extends AbstractHttpServiceHandler {

  public static final String PAGE_RANKS_RANK_HANDLER = "pagerank";

  @UseDataSet(PageRankApp.PAGE_RANK_RANKS_DATASET)
  private ObjectStore<Double> pageRanks;

  @Path(PAGE_RANKS_RANK_HANDLER)
  @GET
  public void getRank(HttpServiceRequest request, HttpServiceResponder responder, @QueryParam("url") String url) {
    if (url == null) {
      responder.sendString(HttpURLConnection.HTTP_BAD_REQUEST,
                           String.format("The url parameter must be specified"), Charsets.UTF_8);
      return;
    }

    Double rank = pageRanks.read(url.getBytes(Charsets.UTF_8));
    if (rank == null) {
      responder.sendString(HttpURLConnection.HTTP_NO_CONTENT,
                           String.format("No rank found of %s", url), Charsets.UTF_8);
    } else {
      responder.sendString(rank.toString());
    }
  }
}
