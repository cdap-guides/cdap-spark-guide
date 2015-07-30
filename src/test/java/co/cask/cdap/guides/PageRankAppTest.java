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

import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Unit test for {@link PageRankApp}
 */
public class PageRankAppTest extends TestBase {

  private static final String URL_PAIR1 = "http://example.com/page1 http://example.com/page1";
  private static final String URL_PAIR2 = "http://example.com/page1 http://example.com/page10";
  private static final String URL_PAIR3 = "http://example.com/page1 http://example.com/page100";
  private static final String URL_PAIR4 = "http://example.com/page10 http://example.com/page10";
  private static final String URL_PAIR5 = "http://example.com/page10 http://example.com/page100";
  private static final String URL_PAIR6 = "http://example.com/page100 http://example.com/page100";

  @Test
  public void test() throws Exception {
    // Deploy the App
    ApplicationManager appManager = deployApplication(PageRankApp.class);

    // Send a stream events to the Stream
    StreamManager streamManager = getStreamManager(PageRankApp.PAGE_RANK_BACKLINK_STREAM);
    streamManager.send(URL_PAIR1);
    streamManager.send(URL_PAIR2);
    streamManager.send(URL_PAIR3);
    streamManager.send(URL_PAIR4);
    streamManager.send(URL_PAIR5);
    streamManager.send(URL_PAIR6);

    // Start the Spark Program
    SparkManager sparkManager = appManager.getSparkManager(PageRankSpark.class.getSimpleName()).start();
    sparkManager.waitForFinish(60, TimeUnit.SECONDS);

    ServiceManager serviceManager = appManager.getServiceManager(PageRankApp.PAGE_RANK_RANKS_SERVICE).start();
    serviceManager.waitForStatus(true);

    //Query for rank and verify it
    URL totalHitsURL = new URL(serviceManager.getServiceURL(15, TimeUnit.SECONDS),
                               PageRankHandler.PAGE_RANKS_RANK_HANDLER);

    HttpResponse response = HttpRequests.execute(HttpRequest.post(totalHitsURL)
                                                   .withBody("{\"url\":\"" + "http://example.com/page1" + "\"}")
                                                   .build());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, response.getResponseCode());
    Assert.assertTrue(Double.parseDouble(response.getResponseBodyAsString()) > 0.0);

    serviceManager.stop();
    serviceManager.waitForStatus(false);
    appManager.stopAll();
  }
}
