package org.folio.rest.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.*;

import java.net.URL;

import org.junit.Test;

import io.vertx.core.http.HttpClientOptions;

public class LoaderAPITest {

  private void assertHttpClientOptions(String url, String host, int port, boolean isSsl) throws Exception {
    HttpClientOptions options = LoaderAPI.createHttpClientOptions(new URL(url));
    assertThat(options.getDefaultHost(), is(host));
    assertThat(options.getDefaultPort(), is(port));
    assertThat(options.isSsl(), is(isSsl));
  }

  @Test
  public void testCreateHttpClientOptions() throws Exception {
    assertHttpClientOptions("http://localhost", "localhost", 80, false);
    assertHttpClientOptions("https://localhost", "localhost", 443, true);
    assertHttpClientOptions("http://localhost:443", "localhost", 443, false);
    assertHttpClientOptions("https://localhost:80", "localhost", 80, true);
    assertHttpClientOptions("http://example.com:9130/okapi", "example.com", 9130, false);
    assertHttpClientOptions("https://example.com:9131/okapi:5/", "example.com", 9131, true);
  }

}
