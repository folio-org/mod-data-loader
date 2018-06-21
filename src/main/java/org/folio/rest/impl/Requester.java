package org.folio.rest.impl;

import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.folio.rest.RestVerticle;

import java.io.IOException;
import java.util.Map;

public class Requester {

  private static final int CONNECT_TIMEOUT = 3 * 1000;
  private static final int CONNECTION_TIMEOUT = 300 * 1000; //keep connection open this long
  private static final int SO_TIMEOUT = 180 * 1000; //during data flow, if interrupted for 180sec, regard connection as
  // stalled/broken.

  public HttpResponse post(String url, StringBuilder data, Map<String, String> okapiHeaders) throws IOException {
    RequestConfig config = RequestConfig.custom()
      .setConnectTimeout(CONNECT_TIMEOUT)
      .setConnectionRequestTimeout(CONNECTION_TIMEOUT)
      .setSocketTimeout(SO_TIMEOUT)
      .build();
    try (CloseableHttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
      HttpPost httpPost = new HttpPost(url);
      StringEntity stringEntity = new StringEntity(data.toString(), "UTF8");
      httpPost.setEntity(stringEntity);
      httpPost.setHeader(RestVerticle.OKAPI_HEADER_TENANT,
        okapiHeaders.get(RestVerticle.OKAPI_HEADER_TENANT));
      httpPost.setHeader(RestVerticle.OKAPI_HEADER_TOKEN,
        okapiHeaders.get(RestVerticle.OKAPI_HEADER_TOKEN));
      httpPost.setHeader(RestVerticle.OKAPI_USERID_HEADER,
        okapiHeaders.get(RestVerticle.OKAPI_USERID_HEADER));
      httpPost.setHeader("Content-type", "application/octet-stream");
      httpPost.setHeader("Accept", "text/plain");
      return httpclient.execute(httpPost);
    }
  }
}
