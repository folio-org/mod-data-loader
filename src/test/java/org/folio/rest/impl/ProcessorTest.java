package org.folio.rest.impl;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.folio.util.ResourceUtil;
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
  private Processor processor1;
  private Processor processor2;
  private Map<String, String> okapiHeaders;
  private JsonObject rulesFile;

  @Mock
  private Requester requester;

  @Before
  public void setUp(TestContext ctx) throws IOException {

    MockitoAnnotations.initMocks(this);
    vertx = Vertx.vertx();
    BasicHttpResponse dummyResponse = createDummyResponse();
    when(requester.post(anyString(), any(), anyMap())).thenReturn(dummyResponse);

    InputStream twoMarcInstances = this.getClass().getResourceAsStream("/sourceRecords/msdb.bib.sub");
    InputStream oneEntryWQuotationM = this.getClass().getResourceAsStream("/sourceRecords/one-entry-with-quotation-marks.mrc");
    rulesFile = new JsonObject(ResourceUtil.asString("rules.json"));
    okapiHeaders = new HashMap<>();

    processor1 = new Processor("testTenantId", okapiHeaders, requester, true,
      "my-test-id");
    processor1.setRulesFile(rulesFile);
    processor1.process(false, twoMarcInstances, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 20);

    processor2 = new Processor("testTenantId", okapiHeaders, requester, true,
      "my-test-id");
    processor2.setRulesFile(rulesFile);
    processor2.process(false, oneEntryWQuotationM, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 5);
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void sqlQueriesTest() throws IOException {
    LOGGER.info("\n---\nsqlQueriesTest()\n---");
    String instancesSqlExpected = ResourceUtil.asString("expected/msdb.bib.sub.instance.query");
    String sourcesSqlExpected   = ResourceUtil.asString("expected/msdb.bib.sub.source.query");
    assertEquals(instancesSqlExpected, processor1.getInstancePostQuery());
    assertEquals(sourcesSqlExpected,   processor1.getSourcePostQuery());
  }

  @Test
  public void escapeQuotationMarksInJsonTest(TestContext ctx) throws IOException {
    LOGGER.info("\n---\nescapeQuotationMarksInJsonTest()\n---");
    String json = ResourceUtil.asString("expected/one-entry-with-quotation-marks.json");
    String jsonExpected = new JsonObject(json).encode();
    assertEquals(jsonExpected, processor2.getSourceRecord().getSourceJson().encode());
  }

  private BasicHttpResponse createDummyResponse() {
    return new BasicHttpResponse(
      new BasicStatusLine(
      new ProtocolVersion("http", 1, 1), 200, "OK")
    );
  }
}
