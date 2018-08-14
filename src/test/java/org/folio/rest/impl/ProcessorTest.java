package org.folio.rest.impl;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.apache.http.ProtocolVersion;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.message.BasicStatusLine;
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
  private Vertx vertx;
  JsonObject rulesFile;

  @Mock
  private Requester requester;

  @Before
  public void setUp(TestContext ctx) throws IOException {

    MockitoAnnotations.initMocks(this);
    vertx = Vertx.vertx();
    BasicHttpResponse dummyResponse = createDummyResponse();
    when(requester.post(anyString(), any(), anyMap())).thenReturn(dummyResponse);

    rulesFile = new JsonObject(ResourceUtil.asString("rules.json"));
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  private BasicHttpResponse createDummyResponse() {
    return new BasicHttpResponse(
      new BasicStatusLine(
      new ProtocolVersion("http", 1, 1), 200, "OK")
    );
  }

  private Processor process(TestContext ctx, String mrcFile, int bulkSize) {
    Async async = ctx.async();
    InputStream twoMarcInstances = this.getClass().getResourceAsStream(mrcFile);
    Map<String, String> okapiHeaders = new HashMap<>();
    Processor processor = new Processor("testTenantId", okapiHeaders, requester, true,
        "my-test-id");
    processor.setRulesFile(rulesFile);
    processor.process(false, twoMarcInstances, vertx.getOrCreateContext(),
        result -> async.complete(), bulkSize);
    async.awaitSuccess();
    return processor;
  }

  private void assertInstancePostQuery(String expectedFile, Processor processor)
      throws IOException {
    assertEquals(ResourceUtil.asString(expectedFile), processor.getInstancePostQuery());
  }

  private void assertSourcePostQuery(String expectedFile, Processor processor)
      throws IOException {
    assertEquals(ResourceUtil.asString(expectedFile), processor.getSourcePostQuery());
  }

  private void assertSourceJson(String expectedFile, Processor processor)
      throws IOException {
    String expected = new JsonObject(ResourceUtil.asString(expectedFile)).encode();
    assertEquals(expected, processor.getSourceRecord().getSourceJson().encode());
  }

  @Test
  public void msdb(TestContext ctx) throws IOException {
    Processor processor = process(ctx, "/sourceRecords/msdb.bib.sub", 20);
    assertInstancePostQuery("expected/msdb.bib.sub.instance.query", processor);
    assertSourcePostQuery  ("expected/msdb.bib.sub.source.query",   processor);
  }

  @Test
  public void oneEntryWithQuotationMarks(TestContext ctx) throws IOException {
    Processor processor = process(ctx, "/sourceRecords/one-entry-with-quotation-marks.mrc", 5);
    assertSourcePostQuery("expected/one-entry-with-quotation-marks-double-escape.query", processor);
    assertSourceJson     ("expected/one-entry-with-quotation-marks.json",                processor);
  }

  @Test
  public void oneEntryFuga(TestContext ctx) throws IOException {
    Processor processor = process(ctx, "/sourceRecords/fuga.mrc", 5);
    assertSourcePostQuery("expected/fuga.query", processor);
    assertSourceJson     ("expected/fuga.json",  processor);
  }
}
