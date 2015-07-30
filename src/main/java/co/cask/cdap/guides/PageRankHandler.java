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
import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.net.HttpURLConnection;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

/**
 * Handles queries retrieving the PageRank of a URL.
 */

public final class PageRankHandler extends AbstractHttpServiceHandler {

  private static final Gson GSON = new Gson();
  public static final String URL_KEY = "url";
  public static final String PAGE_RANKS_RANK_HANDLER = "pagerank";

  @UseDataSet(PageRankApp.PAGE_RANK_RANKS_DATASET)
  private ObjectStore<Double> ranks;

  @Path(PAGE_RANKS_RANK_HANDLER)
  @POST
  public void getRank(HttpServiceRequest request, HttpServiceResponder responder) {
    String url = GSON.fromJson(Charsets.UTF_8.decode(request.getContent()).toString(),
                               JsonObject.class).get(URL_KEY).getAsString();
    if (url == null) {
      responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "The url must be specified with url as key in JSON.");
      return;
    }

    // Get the rank from the ranks dataset
    Double rank = ranks.read(url.getBytes(Charsets.UTF_8));
    if (rank == null) {
      responder.sendError(HttpURLConnection.HTTP_NO_CONTENT, String.format("No rank found of %s", url));
    } else {
      responder.sendString(rank.toString());
    }
  }
}
