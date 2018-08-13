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
  private Processor processor3;

  @Mock
  private Requester requester;

  @Before
  public void setUp(TestContext ctx) throws IOException {

    MockitoAnnotations.initMocks(this);
    vertx = Vertx.vertx();
    BasicHttpResponse dummyResponse = createDummyResponse();
    when(requester.post(anyString(), any(), anyMap())).thenReturn(dummyResponse);

    InputStream twoMarcInstances = this.getClass().getResourceAsStream(
      "/sourceRecords/msdb.bib.sub");
    InputStream oneEntryWQuotationM = this.getClass().getResourceAsStream(
      "/sourceRecords/one-entry-with-quotation-marks.mrc");
    InputStream oneEntry = this.getClass().getResourceAsStream(
      "/sourceRecords/entry-with-unclear-error.mrc");

    JsonObject rulesFile = new JsonObject(ResourceUtil.asString("rules.json"));
    Map<String, String> okapiHeaders = new HashMap<>();

    processor1 = new Processor("testTenantId", okapiHeaders, requester, true,
      "my-test-id");
    processor1.setRulesFile(rulesFile);
    processor2 = new Processor(processor1);
    processor3 = new Processor(processor1);

    processor1.process(false, twoMarcInstances, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 20);
    processor2.process(false, oneEntryWQuotationM, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 5);
    processor3.process(false, oneEntry, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 5);
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void sqlQueriesAndJsonTest() throws IOException {

    LOGGER.info("\n---\nsql query asserts\n---");

    // load files for expected output
    String instancesSqlExpected = ResourceUtil.asString("expected/msdb.bib.sub.instance.query");
    String sourcesSqlExpected   = ResourceUtil.asString("expected/msdb.bib.sub.source.query");
    String escapeQuotesSqlExpected = ResourceUtil.asString("expected/one-entry-with-quotation-marks-double-escape.query");
    String entryUnclearErrorExpected = ResourceUtil.asString("expected/entry-with-unclear-error.query");

    // assert with processed
    assertEquals(instancesSqlExpected, processor1.getInstancePostQuery());
    assertEquals(sourcesSqlExpected,   processor1.getSourcePostQuery());
    assertEquals(escapeQuotesSqlExpected, processor2.getSourcePostQuery());
    assertEquals(entryUnclearErrorExpected, processor3.getSourcePostQuery());

    LOGGER.info("\n---\njson asserts\n---");

    // load files for expected output
    String oneEntryWithQuotationMarks = new JsonObject(
      ResourceUtil.asString("expected/one-entry-with-quotation-marks.json")).encode();
    String entryWithUnclearError = new JsonObject(
      ResourceUtil.asString("expected/entry-with-unclear-error.json")).encode();

    // assert with processed
    assertEquals(oneEntryWithQuotationMarks, processor2.getSourceRecord().getSourceJson().encode());
    assertEquals(entryWithUnclearError, processor3.getSourceRecord().getSourceJson().encode());
  }

  private BasicHttpResponse createDummyResponse() {
    return new BasicHttpResponse(
      new BasicStatusLine(
      new ProtocolVersion("http", 1, 1), 200, "OK")
    );
  }
}
