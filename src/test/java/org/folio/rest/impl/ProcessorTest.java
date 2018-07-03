package org.folio.rest.impl;


import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.IOUtils;
import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class ProcessorTest {

  private static final Logger LOGGER = LogManager.getLogger(ProcessorTest.class);
  private Vertx vertx;
  private Processor processor;

  @Mock
  private Requester requester;

  @Before
  public void setUp(TestContext ctx) throws IOException {

    MockitoAnnotations.initMocks(this);
    vertx = Vertx.vertx();
    BasicHttpResponse dummyResponse = createDummyResponse();
    when(requester.post(anyString(), any(), anyMap())).thenReturn(dummyResponse);

    InputStream twoMarcInstances = this.getClass().getResourceAsStream("/sourceRecords/msdb.bib.sub");

    InputStream rules = this.getClass().getResourceAsStream("/rules.json");
    JsonObject rulesFile = new JsonObject(IOUtils.toString(rules));
    Map<String, String> okapiHeaders = new HashMap<>();

    processor = new Processor("testTenantId", okapiHeaders, requester, true,
      "my-test-id");
    processor.setRulesFile(rulesFile);
    processor.process(false, twoMarcInstances, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 20);
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void sqlQueriesTest() throws IOException {
    LOGGER.info("\n---\nsqlQueriesTest()\n---");
    InputStream twoMarcInstancesSQL = this.getClass().getResourceAsStream("/expected/msdb.bib.sub.query");
    assertEquals(IOUtils.toString(twoMarcInstancesSQL), processor.getLastInstancePostQuery() + "\n");
//    assertEquals("expected query", processor.getLastSourcePostQuery());
  }

  private BasicHttpResponse createDummyResponse() {
    return new BasicHttpResponse(
      new BasicStatusLine(
      new ProtocolVersion("http", 1, 1), 200, "OK")
    );
  }
}
