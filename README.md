Iterative Data Processing with Apache Spark
===========================================

[Apache Spark](https://spark.apache.org/) is a very popular engine to
perform in-memory cluster computing for Apache Hadoop. In this guide,
you will learn how to run Apache Spark programs with CDAP.

What You Will Build
-------------------

You will build a 
[CDAP application](http://docs.cdap.io/cdap/current/en/developer-guide/building-blocks/applications.html)
that exposes a REST API to take in web pages’ backlinks information and
serve out the [PageRank](http://en.wikipedia.org/wiki/PageRank) for the
known web pages. You will:

- Build a CDAP
  [Spark](http://docs.cdap.io/cdap/2.5.0/en/developer-guide/building-blocks/spark-jobs.html)
  program that computes the PageRank of the web pages;
- Build a
  [Service](http://docs.cdap.io/cdap/current/en/developer-guide/building-blocks/services.html)
  to receive backlinks data and serve the PageRank computation results over HTTP;
- Use a
  [Dataset](http://docs.cdap.io/cdap/current/en/developer-guide/building-blocks/datasets/index.html)
  to store the input data; and
- Use a Dataset as input and output for the Spark program.

What You Will Need
------------------

- [JDK 6 or JDK 7](http://www.oracle.com/technetwork/java/javase/downloads/index.html)
- [Apache Maven 3.0+](http://maven.apache.org/)
- [CDAP SDK](http://docs.cdap.io/cdap/current/en/developer-guide/getting-started/standalone/index.html)

Let’s Build It!
---------------

The following sections will guide you through building an application from scratch. If you
are interested in deploying and running the application right away, you can clone its
source code from this GitHub repository. In that case, feel free to skip the next two
sections and jump right to the 
[Build and Run Application]() section.

### Application Design

Backlinks data is sent to the *PageRankService* over HTTP (e.g. by a web
crawler as it processes web pages). The service persists the data into a
*backLinks* dataset upon receiving it. The PageRank for known pages is
computed periodically by a *PageRankProgram*. The program uses the
*backLinks* dataset as an input and persists the results in the
*pageRanks* dataset.

The *PageRankService* then uses the *pageRanks* dataset to serve the
PageRank for a given URL over HTTP.

In this guide we assume that the backlinks data will be sent to a CDAP
application.

![](docs/images/app-design.png)

### Implementation

The first step is to construct the application structure. We will use a
standard Maven project structure for all of the source code files:

    ./pom.xml
    ./src/main/java/co/cask/cdap/guides/BackLinksHandler.java
    ./src/main/java/co/cask/cdap/guides/PageRankApp.java
    ./src/main/java/co/cask/cdap/guides/PageRankSpark.java
    ./src/main/java/co/cask/cdap/guides/PageRankHandler.java
    ./src/main/scala/co/cask/cdap/guides/PageRankProgram.scala

The application is identified by the `PageRankApp` class. This class
extends
[AbstractApplication](http://docs.cdap.io/cdap/2.5.0/en/reference/javadocs/co/cask/cdap/api/app/AbstractApplication.html),
and overrides the `configure( )` method to define all of the application components:

```java
public class PageRankApp extends AbstractApplication {

  @Override
  public void configure() {
    setName("PageRankApplication");
    addSpark(new PageRankSpark());
    addService("PageRankService", new ImmutableList.Builder<HttpServiceHandler>()
      .add(new BackLinksHandler())
      .add(new PageRankHandler())
      .build());
    try {
      ObjectStores.createObjectStore(getConfigurer(), "backLinks", String.class);
      ObjectStores.createObjectStore(getConfigurer(), "pageRanks", Double.class);
    } catch (UnsupportedTypeException e) {
      throw new RuntimeException("Won't happen: all classes above are supported", e);
    }
  }
}
```

In this example we’ll use Scala to write a Spark program (for an example
of using Java, refer to the [CDAP SparkPageRank
example](http://docs.cask.co/cdap/current/en/developer-guide/examples/spark-page-rank.html)).
You’ll need to add `scala` and `maven-scala-plugin` as dependencies in
your Maven
[pom.xml.](https://github.com/cdap-guides/cdap-spark-guide/blob/develop/pom.xml)

The code below configures Spark in CDAP. This class extends
[AbstractSpark](http://docs.cdap.io/cdap/current/en/reference/javadocs/co/cask/cdap/api/spark/AbstractSpark.html)
and overrides the `configure( )` method to define all of the components. The
`setMainClassName` method sets the Spark Program class which CDAP will run:

```java
public class PageRankSpark extends AbstractSpark {

  @Override
  public SparkSpecification configure() {
    return SparkSpecification.Builder.with()
      .setName("PageRankProgram")
      .setDescription("Spark program to compute PageRank")
      .setMainClassName(PageRankProgram.class.getName())
      .build();
  }
}
```

`BackLinksHandler` receives backlinks info via POST to `backlink`. Valid
backlink information is in the form of two URLs separated by whitespace:

``` {.sourceCode .console}
http://example.com/page1 http://example.com/page10
```

The `BackLinksHandler` stores the backlink information in an [ObjectStore
Dataset](http://docs.cask.co/cdap/current/en/reference/javadocs/co/cask/cdap/api/dataset/lib/ObjectStore.html)
as a String in the format shown above:

```java
public class BackLinksHandler extends AbstractHttpServiceHandler {

  @UseDataSet("backLinks")
  private ObjectStore<String> backLinks;

  @Path("backlink")
  @POST
  public void handleBackLink(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();

    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.NO_CONTENT.code(), "Request content is empty.");
      return;
    }

    if (parseAndStore(Charsets.UTF_8.decode(requestContents).toString().trim())) {
      responder.sendStatus(HttpResponseStatus.OK.code());
    } else {
      responder.sendError(HttpResponseStatus.BAD_REQUEST.code(), "Malformed backlink information");
    }
  }

  /**
   * Validates the format and stores the backlink information if valid
   *
   * @param bLink the request body
   * @return true if the backlink information is valid else false
   */
  private boolean parseAndStore(String bLink) {
    String[] backlinkURLs = bLink.split("\\s+");
    if (backlinkURLs.length == 2) {
      backLinks.write(bLink, bLink);
      return true;
    }
    return false;
  }
}
```

The `PageRankProgram` Spark program does the actual page rank
computation. This code is taken from the [Apache Spark's PageRank
example](https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/SparkPageRank.scala);
the Spark program stores the computed PageRank in an ObjectStore
Dataset where the key is the URL and the value is the computed PageRank:

```java
class PageRankProgram extends ScalaSparkProgram {

  private final val ITERATIONS_COUNT: Int = 10

  override def run(sc: SparkContext) {
    val lines: RDD[(Array[Byte], String)] = sc.readFromDataset("backLinks", classOf[Array[Byte]], classOf[String])
    val links = lines.map { s =>
      val parts = s._2.split("\\s+")
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
```

To serve results out via HTTP, add a `PageRankHandler`, which
reads the PageRank for a given URL from the `pageRanks` dataset:

```java
public class PageRankHandler extends AbstractHttpServiceHandler {

  @UseDataSet("pageRanks")
  private ObjectStore<Double> pageRanks;

  @Path("pagerank")
  @POST
  public void handleBackLink(HttpServiceRequest request, HttpServiceResponder responder) {

    ByteBuffer requestContents = request.getContent();
    if (requestContents == null) {
      responder.sendError(HttpResponseStatus.NO_CONTENT.code(), "No URL provided.");
      return;
    }

    String urlParam = Charsets.UTF_8.decode(requestContents).toString();

    Double rank = pageRanks.read(urlParam);
    if (rank == null) {
      responder.sendError(HttpResponseStatus.NOT_FOUND.code(), "The following URL was not found: " + urlParam);
      return;
    }

    responder.sendJson(String.valueOf(rank));
  }
}
```

Build and Run Application
-------------------------

The `PageRankApp` application can be built and packaged using the Apache Maven command:

    mvn clean package

Note that the remaining commands assume that the `cdap-cli.sh` script is
available on your PATH. If this is not the case, please add it:

    export PATH=$PATH:<CDAP home>/bin

If you haven't already started a standalone CDAP installation, start it with the command:

    cdap.sh start

You can then deploy the application to a standalone CDAP installation:

    cdap-cli.sh deploy app target/cdap-spark-guide-1.0.0.jar

Start the Service:

    cdap-cli.sh start service PageRankApp.PageRankService 

Send some Data:

    export BACKLINK_URL=http://localhost:10000/v2/apps/PageRankApp/services/PageRankService/methods/backlink

    curl -v -X POST -d 'http://example.com/page1 http://example.com/page1' $BACKLINK_URL  
    curl -v -X POST -d 'http://example.com/page1 http://example.com/page10' $BACKLINK_URL  
    curl -v -X POST -d 'http://example.com/page10 http://example.com/page10' $BACKLINK_URL  
    curl -v -X POST -d 'http://example.com/page10 http://example.com/page100' $BACKLINK_URL  
    curl -v -X POST -d 'http://example.com/page100 http://example.com/page100' $BACKLINK_URL

Run the Spark Program:

    curl -v -X POST 'http://localhost:10000/v2/apps/PageRankApp/spark/PageRankProgram/start'

The Spark Program can take time to complete. You can check the status
for completion using:

    curl -v 'http://localhost:10000/v2/apps/PageRankApp/spark/PageRankProgram/status'

Query for the PageRank results:

    curl -v -d 'http://example.com/page10' -X POST 'http://localhost:10000/v2/apps/PageRankApp/services/PageRankService/methods/pagerank'

Example output:

    0.45521228811700043

Congratulations! You have now learned how to incorporate Spark programs
into your CDAP applications. Please continue to experiment and extend
this sample application.

Share and Discuss!
------------------

Have a question? Discuss at the [CDAP User Mailing List.](https://groups.google.com/forum/#!forum/cdap-user)

License
-------

Copyright © 2014 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may
not use this file except in compliance with the License. You may obtain
a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
