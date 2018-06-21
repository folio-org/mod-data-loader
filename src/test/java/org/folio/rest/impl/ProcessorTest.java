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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import static org.mockito.Mockito.*;

import javax.ws.rs.core.Response;
import java.io.*;
import java.util.HashMap;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class ProcessorTest {

//  @Rule
//  public RunTestOnContext rule = new RunTestOnContext();

  private Vertx vertx;
  private Processor processor;
  private InputStream marcInputStream;
  private BasicHttpResponse dummyResponse;

  @Mock
  private Handler<AsyncResult<Response>> asyncResultHandler;

  @Mock
  private Requester requester;

  @Before
  public void setUp() throws IOException {

    MockitoAnnotations.initMocks(this);
    vertx = Vertx.vertx();
    marcInputStream = this.getClass().getResourceAsStream("/msplit00000000.mrc");

    Map<String, String> okapiHeaders = new HashMap<>();
    processor = new Processor("testTenantId", okapiHeaders, requester);
    dummyResponse = createDummyResponse();

    InputStream rules = this.getClass().getResourceAsStream("/rules.json");
    JsonObject rulesFile = new JsonObject(IOUtils.toString(rules));
    processor.setRulesFile(rulesFile);
  }

  @After
  public void tearDown(TestContext ctx) {
    vertx.close(ctx.asyncAssertSuccess());
  }

  @Test
  public void sourceRecordHandlingTest(TestContext context) throws IOException {

    when(requester.post(anyString(), any(), anyMap())).thenReturn(dummyResponse);
    processor.process(false, marcInputStream, vertx.getOrCreateContext(), context.asyncAssertSuccess(), 200);

  }

  private BasicHttpResponse createDummyResponse() {
    return new BasicHttpResponse(
      new BasicStatusLine(
      new ProtocolVersion("http", 1, 1), 200, "OK")
    );
  }
}
