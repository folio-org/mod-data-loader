package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import javax.ws.rs.core.Response;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class ProcessorTest {

//  @Rule
//  public RunTestOnContext rule = new RunTestOnContext();
  private static final Logger LOGGER = LogManager.getLogger(ProcessorTest.class);
  private Vertx vertx;
  private Processor processor;
  private Processor processorSQLQueryTest;
  private InputStream marcInputStream;
  private InputStream twoMarcInstances;
  private String twoMarcInstancesOut;
  private BasicHttpResponse dummyResponse;

  @Mock
  private Handler<AsyncResult<Response>> asyncResultHandler;

  @Mock
  private Requester requester;

  @Before
  public void setUp(TestContext ctx) throws IOException {

    MockitoAnnotations.initMocks(this);
    vertx = Vertx.vertx();
    dummyResponse = createDummyResponse();
    when(requester.post(anyString(), any(), anyMap())).thenReturn(dummyResponse);

    marcInputStream = this.getClass().getResourceAsStream("/msplit00000000.mrc");
    twoMarcInstances = this.getClass().getResourceAsStream("/sourceRecords/msdb.bib.sub");

    try (InputStream is = this.getClass().getResourceAsStream("/expected/msdb.bib.sub.out")) {
      twoMarcInstancesOut = IOUtils.toString(is);
    }

    Map<String, String> okapiHeaders = new HashMap<>();
    processor = new Processor("testTenantId", okapiHeaders, requester, false, null);

    InputStream rules = this.getClass().getResourceAsStream("/rules.json");
    JsonObject rulesFile = new JsonObject(IOUtils.toString(rules));
    processor.setRulesFile(rulesFile);

    processorSQLQueryTest = new Processor("testTenantId", okapiHeaders, requester, false,
      "my-test-id");
    processorSQLQueryTest.setRulesFile(rulesFile);
    processorSQLQueryTest.process(true, twoMarcInstances, vertx.getOrCreateContext(), ctx.asyncAssertSuccess(), 20);
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void sourceRecordHandlingTest(TestContext context) throws IOException {
    LOGGER.info("\n---\nsourceRecordHandlingTest()\n---");
    processor.setStoreSource(true);

    processor.process(false, marcInputStream, vertx.getOrCreateContext(), context.asyncAssertSuccess(), 200);

    processor.setStoreSource(false);
  }

  @Test
  public void sqlQueryTest() {
    LOGGER.info("\n---\nsqlQueryTest()\n---");
    assertEquals(twoMarcInstancesOut, processorSQLQueryTest.getImportSQLStatement());
  }

  private BasicHttpResponse createDummyResponse() {
    return new BasicHttpResponse(
      new BasicStatusLine(
      new ProtocolVersion("http", 1, 1), 200, "OK")
    );
  }
}
