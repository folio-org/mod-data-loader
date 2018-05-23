package org.folio;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Mtype;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.messages.Messages;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.VertxUtils;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import static org.junit.Assert.*;

public class RulesTest {
  private static final Logger log = LoggerFactory.getLogger(Messages.class);

  private static Vertx vertx;
  private static int port;
  private HttpClient client = vertx.createHttpClient();
  private static Locale oldLocale = Locale.getDefault();
  private ObjectMapper jsonMapper = ObjectMapperTool.getMapper();

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4jLogDelegateFactory");
  }

  @BeforeClass
  public static void setUp() {
    vertx = VertxUtils.getVertxWithExceptionHandler();
    port = NetworkUtils.nextFreePort();

    try {
      deployRestVerticle();
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  private static void deployRestVerticle() {

    CompletableFuture<String> deploymentComplete = new CompletableFuture<>();

    DeploymentOptions deploymentOptions = new DeploymentOptions().setConfig(
        new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), deploymentOptions, res -> {
      if(res.succeeded()) {
        deploymentComplete.complete(res.result());
      }
      else {
        deploymentComplete.completeExceptionally(res.cause());
      }
    });

    //wait for verticle to complete startup
    try {
      deploymentComplete.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }
  }

  /**
   * Cleanup: Delete temporary file, restore Locale, close the vert.x instance.
   *
   */
  @AfterClass
  public static void tearDown() {
    CompletableFuture<String> deploymentComplete = new CompletableFuture<>();

    deleteTempFilesCreated();
    Locale.setDefault(oldLocale);
    vertx.close(  res -> {
      if(res.succeeded()) {
        deploymentComplete.complete("success");
      }
      else {
        deploymentComplete.completeExceptionally(res.cause());
      }
    });
  }

  private static void deleteTempFilesCreated(){
    log.info("deleting created files");
    // Lists all files in folder
    File folder = new File(RestVerticle.DEFAULT_TEMP_DIR);
    File[] fList = folder.listFiles();
    // Searchs test.json
    for (File file : fList) {
      String pes = file.getName();
      if (pes.endsWith("test.json")) {
        // and deletes
        if (file.delete()) {
          System.out.println("Deleted test.json");
        }
      }
    }
  }

  @Test
  public void testLoading() throws Exception {

    System.out.println(" Running.... testLoading()");

    String rules = getFile("rules.json");
    List<String> lines = getFileAsList("instanceObjects");

    CompletableFuture<Response> loadRulesCF = new CompletableFuture<>();
    CompletableFuture<TextResponse> loadDataCf = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/marc-rules", rules, empty(loadRulesCF));
    Response r = loadRulesCF.get();
    System.out.println("response for loading marc-rules is: " + r.getStatusCode());
    assertEquals(201, r.getStatusCode());

    String data = getFile("msplit00000000.mrc");

    postData("http://localhost:" + port + "/load/marc-data/test", data, text(loadDataCf));
    TextResponse t = loadDataCf.get();
    System.out.println("response for loading marc-data is: " + t.getStatusCode());
    assertEquals(201, t.getStatusCode());
    List<String> body = getBodyAsList(t.getBody());
    System.out.print("comparing line....");

    for(int i=0; i<lines.size(); i++){

      Instance instance = jsonMapper.readValue(body.get(i).substring(body.get(i).indexOf("|")+1), Instance.class);
      assertUuid(instance.getId());
      instance.setId(null);
      assertEqualObject(lines, i, instance);
    }
    System.out.println("all "+lines.size()+" lines matched...");
  }

  private void assertEqualObject(List<String> lines, int lineIndex, Object object) {

    int lineNumber = lineIndex + 1;

    try {
      System.out.print((lineNumber) + " ");
      JsonAssert.areEqual(lines.get(lineIndex), PostgresClient.pojo2json(object));
    } catch (Exception e) {
      System.out.println("error at " + lineNumber);
      e.printStackTrace();
    }
  }

  @Test
  public void testLoadingNoTenant() throws Exception {

    System.out.println(" Running.... testLoadingNoTenant()");

    List<String> lines = getFileAsList("instanceObjects");

    CompletableFuture<TextResponse> loadDataCf = new CompletableFuture<>();

    String data = getFile("msplit00000000.mrc");

    postData("http://localhost:" + port + "/load/marc-data/test", data, false, text(loadDataCf));
    TextResponse t = loadDataCf.get();
    System.out.println("response for loading marc-data is: " + t.getStatusCode());
    assertEquals(400, t.getStatusCode());
  }

  @Test
  public void testStaticLoading() throws Exception {

    System.out.println(" Running.... testStaticLoading()");

    String materialType = getFile("mapping/static/material_type");
    List<String> lines = getFileAsList("materialTypeObjects");

    CompletableFuture<TextResponse> materialTypeCF = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/static/test", materialType, text(materialTypeCF));
    TextResponse t = materialTypeCF.get();
    System.out.println("response for static material type data load is: " + t.getStatusCode());
    assertEquals(201, t.getStatusCode());
    List<String> body = getBodyAsList(t.body);
    System.out.print("OUTPUT: " + t.getBody());
    for(int i=0; i<lines.size(); i++){
      Mtype mtype = jsonMapper.readValue(
        body.get(i).substring(body.get(i).indexOf("|")+1), Mtype.class);
      assertUuid(mtype.getId());
      mtype.setId(null);

      assertEqualObject(lines, i, mtype);
    }
  }

  @Test
  public void testStaticLoadingObjects() throws Exception {

    System.out.println(" Running.... testStaticLoadingObjects()");

    String objectType = getFile("mapping/static/inject_object");
    List<String> lines = getFileAsList("objectTemplateObjects");

    CompletableFuture<TextResponse> objectTypeCF = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/static/test", objectType, text(objectTypeCF));
    TextResponse t = objectTypeCF.get();
    System.out.println("response for static object type data load is: " + t.getStatusCode());
    System.out.print("OUTPUT: " + t.getBody());
    assertEquals(201, t.getStatusCode());
    List<String> body = getBodyAsList(t.body);
    for(int i=0; i<body.size(); i++){
      try {
        System.out.print((i+1) + " ");
        if(!body.get(i).contains(lines.get(i))){
          System.out.println("error at " + (i+1));
          System.out.println("when comparing " + body.get(i) + " and " + lines.get(i));
          fail();
        }
      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }
    assertTrue(true);
  }

  @Test
  public void testStaticLoadingNoRecord() throws Exception {

    System.out.println(" Running.... testStaticLoadingNoRecord()");

    String objectType = getFile("mapping/static/no_record_entry");

    CompletableFuture<TextResponse> badCF = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/static/test", objectType, text(badCF));
    TextResponse t = badCF.get();
    System.out.println("response for bad static data load is: " + t.getStatusCode());
    System.out.print("OUTPUT: " + t.getBody());
    assertEquals(400, t.getStatusCode());
  }

  @Test
  public void testStaticLoadingBadJson() throws Exception {

    System.out.println(" Running.... testStaticLoadingBadJson()");

    String objectType = getFile("mapping/static/badjson");

    CompletableFuture<TextResponse> badCF = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/static/test", objectType, text(badCF));
    TextResponse t = badCF.get();
    System.out.println("response for bad static data load is: " + t.getStatusCode());
    System.out.print("OUTPUT: " + t.getBody());
    assertEquals(400, t.getStatusCode());
  }

  @Test
  public void testStaticLoadingStaticId() throws Exception {

    System.out.println(" Running.... testStaticLoadingStaticId()");

    String objectType = getFile("mapping/static/idexists");

    CompletableFuture<TextResponse> objectTypeCF = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/static/test", objectType, text(objectTypeCF));
    TextResponse t = objectTypeCF.get();
    System.out.println("response for static object type data load is: " + t.getStatusCode());
    System.out.print("OUTPUT: " + t.getBody());
    assertEquals(201, t.getStatusCode());
    List<String> body = getBodyAsList(t.body);
    if(!body.get(0).contains("9d5f9eb6-b92e-4a1a-b4f5-310bc38dacfd")){
      fail("Expected id to be 9d5f9eb6-b92e-4a1a-b4f5-310bc38dacfd");
    }
    if(!body.get(1).contains("9d5f9eb6-b92e-4a1a-b4f5-310bc38dacfc")){
      fail("Expected id to be 9d5f9eb6-b92e-4a1a-b4f5-310bc38dacfc");
    }
    if(!body.get(2).contains("9d5f9eb6-b92e-4a1a-b4f5-310bc38dacfb")){
      fail("Expected id to be 9d5f9eb6-b92e-4a1a-b4f5-310bc38dacfb");
    }
    assertTrue(true);
  }

  @Test
  public void testStaticLoadingNoValues() throws Exception {

    System.out.println(" Running.... testStaticLoadingNoValues()");

    String objectType = getFile("mapping/static/zero_values_array");

    CompletableFuture<TextResponse> objectTypeCF = new CompletableFuture<>();

    postData("http://localhost:" + port + "/load/static/test", objectType, text(objectTypeCF));
    TextResponse t = objectTypeCF.get();
    System.out.println("response for static object type data load is: " + t.getStatusCode());
    System.out.print("OUTPUT: " + t.getBody());
    assertEquals(400, t.getStatusCode());
  }

  private String getFile(String filename) throws IOException {
    return IOUtils.toString(getClass().getClassLoader().getResourceAsStream(filename), "UTF-8");
  }

  private List<String> getFileAsList(String filename) throws IOException {
    return IOUtils.readLines(getClass().getClassLoader().getResourceAsStream(filename), "UTF-8");
  }

  private List<String> getBodyAsList(String data) throws IOException {
    return IOUtils.readLines(new StringReader(data));
  }

  private void postData(String url, String content, Handler<HttpClientResponse> responseHandler){
    postData(url, content, true, responseHandler);
  }

  private void postData(String url, String content, boolean withTenant, Handler<HttpClientResponse> responseHandler){
    HttpClientRequest request = client.postAbs(url, responseHandler);
    request.headers().add("Accept","application/json, text/plain");
    request.headers().add("Content-type","application/octet-stream");
    if(withTenant){
      request.headers().add("x-okapi-tenant","ABC");
    }
    request.end(content);
  }

  private static void assertUuid(String uuid) {
    assertNotNull(uuid);
    assertThat("UUID xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx where x is [0-9a-f]",
        uuid, Matchers.matchesPattern("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
  }

  static Handler<HttpClientResponse> empty(CompletableFuture<Response> completed) {

      return response -> {
        try {
          int statusCode = response.statusCode();

          completed.complete(new RulesTest.Response(statusCode));
        }
        catch(Exception e) {
          completed.completeExceptionally(e);
        }
      };
    }

  public static Handler<HttpClientResponse> text(CompletableFuture<TextResponse> completed) {

    return response -> {
        int statusCode = response.statusCode();

        response.bodyHandler(buffer -> {
          try {
            String body = buffer.toString("UTF8");

            completed.complete(new TextResponse(statusCode, body));

          } catch (Exception e) {
            completed.completeExceptionally(e);
          }
        });
    };
  }

  static class Response {
    private final int statusCode;

    Response(int statusCode) {
      this.statusCode = statusCode;
    }

    int getStatusCode() {
      return statusCode;
    }
  }

  static class TextResponse extends Response {
    private final String body;

    TextResponse(int statusCode, String body) {
      super(statusCode);
      this.body = body;
    }

    String getBody() {
      return body;
    }

    @Override
    public String toString() {
      return String.format("Status Code: %s Body: %s",
        getStatusCode(), getBody());
    }
  }

}
