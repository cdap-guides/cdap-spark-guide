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
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
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
    StreamWriter streamWriter = appManager.getStreamWriter(PageRankApp.BACKLINK_STREAM);
    streamWriter.send(URL_PAIR1);
    streamWriter.send(URL_PAIR2);
    streamWriter.send(URL_PAIR3);
    streamWriter.send(URL_PAIR4);
    streamWriter.send(URL_PAIR5);
    streamWriter.send(URL_PAIR6);


    // Start the Spark Program
    SparkManager sparkManager = appManager.startSpark(PageRankSparkProgram.class.getSimpleName());
    sparkManager.waitForFinish(60, TimeUnit.SECONDS);

    ServiceManager serviceManager = appManager.startService(PageRankApp.RANKS_SERVICE);
    // Wait service startup
    serviceStatusCheck(serviceManager, true);

    String response = requestService(new URL(serviceManager.getServiceURL(), PageRankHandler.PAGE_RANKS_RANK_HANDLER +
      "?url=http://example.com/page1"));

    //pagerank in any case should be more than 0.0
    Assert.assertTrue(Double.parseDouble(response) > 0.0);

    appManager.stopAll();
  }

  private String requestService(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    try {
      return new String(ByteStreams.toByteArray(conn.getInputStream()), Charsets.UTF_8);
    } finally {
      conn.disconnect();
    }
  }

  private void serviceStatusCheck(ServiceManager serviceManger, boolean running) throws InterruptedException {
    int trial = 0;
    while (trial++ < 5) {
      if (serviceManger.isRunning() == running) {
        return;
      }
      TimeUnit.SECONDS.sleep(1);
    }
    throw new IllegalStateException("Service state not executed. Expected " + running);
  }
}
