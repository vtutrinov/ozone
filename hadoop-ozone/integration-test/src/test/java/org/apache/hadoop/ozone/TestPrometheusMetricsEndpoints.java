/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone;


import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.apache.hadoop.hdds.HddsConfigKeys;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.OptionalInt;
import java.util.concurrent.TimeoutException;

import static org.apache.hadoop.hdds.HddsUtils.getPortNumberFromConfigKeys;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Test prometheus http endpoints.
 */
public class TestPrometheusMetricsEndpoints {

  private final OkHttpClient httpClient = new OkHttpClient();

  @Test
  public void testRatisDropwizardMetricsExposedInDefaultPromHttpEndpoint() {
    try {
      configPropertyAwareTestCase("false",
          () -> {
          // given
          OzoneConfiguration conf = new OzoneConfiguration();
          try (MiniOzoneCluster cluster = MiniOzoneCluster.newHABuilder(conf)
              .setNumDatanodes(1)
              .build()) {
            cluster.waitForClusterToBeReady();

            OptionalInt portNumberFromConfigKeys = getPortNumberFromConfigKeys(
                cluster.getHddsDatanodes().get(0).getConf(), HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY);
            String datanodeHttpServerUrl = "http://localhost:" + portNumberFromConfigKeys.getAsInt();

            // when
            Response metricsResponse = httpClient.newCall(new Request.Builder()
                .url(datanodeHttpServerUrl + "/prom")
                .build()).execute();
            String metricsResponseBodyContent = metricsResponse.body().string();


            // then
            assertTrue(metricsResponseBodyContent.contains("#Dropwizard metrics"));
            assertTrue(metricsResponseBodyContent.contains("ratis_server_follower_entry_latency_heartbeat"));

            // and when
            metricsResponse = httpClient.newCall(new Request.Builder()
                .url(datanodeHttpServerUrl + "/prom_ratis")
                .build()).execute();

            // then
            assertEquals(404, metricsResponse.code());
          } catch (Exception e) {
            fail("Something went wrong", e);
          }
        });
    } catch (Exception e) {
      fail("Something went wrong", e);
    }
  }

  @Test
  public void testRatisDropwizardMetricsExposedInIsolatedHttpEndpoint() {
    try {
      configPropertyAwareTestCase("true",
          () -> {
            OzoneConfiguration conf = new OzoneConfiguration();

            try (MiniOzoneCluster cluster = MiniOzoneCluster.newHABuilder(conf)
                .setNumDatanodes(1)
                .build()) {
              cluster.waitForClusterToBeReady();

              OptionalInt portNumberFromConfigKeys = getPortNumberFromConfigKeys(
                  cluster.getHddsDatanodes().get(0).getConf(), HddsConfigKeys.HDDS_DATANODE_HTTP_ADDRESS_KEY);
              String datanodeHttpServerUrl = "http://localhost:" + portNumberFromConfigKeys.getAsInt();

              // when
              Response metricsResponse = httpClient.newCall(new Request.Builder()
                  .url(datanodeHttpServerUrl + "/prom")
                  .build()).execute();
              String metricsResponseBodyContent = metricsResponse.body().string();

              //then
              assertFalse(metricsResponseBodyContent.contains("#Dropwizard metrics"));
              assertFalse(metricsResponseBodyContent.contains("ratis_server_follower_entry_latency_heartbeat"));

              // and when
              metricsResponse = httpClient.newCall(new Request.Builder()
                  .url(datanodeHttpServerUrl + "/prom_ratis")
                  .build()).execute();
              metricsResponseBodyContent = metricsResponse.body().string();

              // then
              assertTrue(metricsResponseBodyContent.contains("#Dropwizard metrics"));
              assertTrue(metricsResponseBodyContent.contains("ratis_server_follower_entry_latency_heartbeat"));
            } catch (IOException | InterruptedException | TimeoutException e) {
              fail("Something went wrong", e);
            }
          });
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void configPropertyAwareTestCase(String confPropertyValue, Runnable test)
      throws ParserConfigurationException, IOException, SAXException, TransformerException {
    try (InputStream resourceAsStream = getClass().getResourceAsStream("/ozone-site.xml")) {
      DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
      Document document = factory.newDocumentBuilder().parse(resourceAsStream);
      Element root = document.getDocumentElement();
      Element property = document.createElement("property");
      Element name = document.createElement("name");
      name.appendChild(document.createTextNode(
          OzoneConfigKeys.OZONE_RATIS_DROPWIZARD_METRICS_USE_ISOLATED_HTTP_ENDPOINT));
      property.appendChild(name);
      Element value = document.createElement("value");
      value.appendChild(document.createTextNode(confPropertyValue));
      property.appendChild(value);
      root.appendChild(property);
      document.normalize();
      TransformerFactory transformerFactory = TransformerFactory.newInstance();
      Transformer transformer = transformerFactory.newTransformer();
      DOMSource source = new DOMSource(document);
      StreamResult result = new StreamResult(new OutputStreamWriter(new FileOutputStream(getClass()
          .getResource("/ozone-site.xml").getPath()), StandardCharsets.UTF_8));
      transformer.transform(source, result);

      test.run();

      root.removeChild(property);
      result = new StreamResult(new OutputStreamWriter(
          new FileOutputStream(getClass().getResource("/ozone-site.xml").getPath()), StandardCharsets.UTF_8));
      transformer.transform(source, result);
      result.getWriter().close();
    }
  }

}
