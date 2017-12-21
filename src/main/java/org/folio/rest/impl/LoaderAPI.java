package org.folio.rest.impl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.folio.rest.RestVerticle;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.resource.LoadResource;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.DataField;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class LoaderAPI implements LoadResource {

  private static final String IMPORT_URL = "/admin/importSQL";

  private int bulkSize = 50000;

  private static final Logger log = LogManager.getLogger(LoaderAPI.class);

  // rules are not stored in db as this is a test loading module
  private static final Map<String, JsonObject> tenantRulesMap = new HashMap<>();

  private int counter;
  private int processedCount;
  private HttpClientInterface client;
  private String url;
  private StringBuffer importSQLStatement = new StringBuffer();

  @Override
  public void postLoadMarcRules(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if (tenantId == null) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcRulesResponse.withPlainBadRequest("tenant not set")));
      return;
    }
    String sqlFile = IOUtils.toString(entity, "UTF8");

    try {
      tenantRulesMap.put(tenantId, new JsonObject(sqlFile));
    } catch (Exception e) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcRulesResponse.withPlainBadRequest("File is not a valid json: " + e.getMessage())));
      return;
    }

    asyncResultHandler.handle(
      io.vertx.core.Future.succeededFuture(PostLoadMarcRulesResponse.withCreated("")));
  }

  @Override
  public void getLoadMarcRules(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if (tenantId == null) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcRulesResponse.withPlainBadRequest("tenant not set")));
      return;
    }
    OutStream stream = new OutStream();
    stream.setData(tenantRulesMap.get(tenantId));
    asyncResultHandler.handle(
      io.vertx.core.Future.succeededFuture(GetLoadMarcRulesResponse.withJsonOK(stream)));

  }

  @Override
  public void getLoadMarcData(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      GetLoadMarcDataResponse.withPlainMethodNotAllowed("Not implemented")));
  }

  @Validate
  @Override
  public void postLoadMarcData(String storageURL, int bulkSize, InputStream entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    long start = System.currentTimeMillis();

    this.bulkSize = bulkSize;

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if (tenantId == null) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest("tenant not set")));
      return;
    }

    JsonObject rules = tenantRulesMap.get(tenantId);

    if(rules == null){
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest("no rules file found for tenant " + tenantId)));
      return;
    }

    url = storageURL;

    client = HttpClientFactory.getHttpClient(storageURL, tenantId);

    //check if inventory storage is respnding
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "text/plain");
    CompletableFuture<org.folio.rest.tools.client.Response> resp = client.request( "/admin/health" , headers );
    resp.whenComplete( (response, error) -> {
      if(error != null){
        log.error(error.getCause());
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
          PostLoadMarcDataResponse.withPlainBadRequest("Unable to connect to the inventory storage module..." + error.getMessage())));
        return;
      }
      if(response.getCode() != 200){
        log.error("Unable to connect to the inventory storage module at..." + storageURL);
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
          PostLoadMarcDataResponse.withPlainBadRequest("Unable to connect to the inventory storage module at..." + storageURL)));
        return;
      }
      else{
        vertxContext.owner().executeBlocking( block -> {
          log.info("REQUEST ID " + UUID.randomUUID().toString());
          try {
          final MarcStreamReader reader = new MarcStreamReader(entity);
          StringBuffer unprocessed = new StringBuffer();
          Object object = new Instance();
          while (reader.hasNext()) {
            processedCount++;
            List<DataField> df = null;
            try {
              df = reader.next().getDataFields();
            } catch (Exception e) {
              unprocessed.append("#").append(processedCount).append(" ");
              log.error(e);
              continue;
            }
            Iterator<DataField> iter = df.iterator();
            object = new Instance();
            while (iter.hasNext()) {
              // this is an iterator on the marc record, field by field
              boolean createNewComplexObj = true; // each rule will generate a new object in an array , for an array data member
              Object rememberComplexObj[] = new Object[] { null };
              DataField dataField = iter.next();
              JsonArray ruleForField = rules.getJsonArray(dataField.getTag());
              if (ruleForField != null) {
                for (int i = 0; i < ruleForField.size(); i++) {
                  JsonObject subFieldRule = ruleForField.getJsonObject(i);
                  JsonArray subFieldsToApplyToRule = subFieldRule.getJsonArray("subfield");
                  StringBuffer sb = new StringBuffer();
                  for (int j = 0; j < subFieldsToApplyToRule.size(); j++) {
                    // get the field->subfield that is associated with the field->subfield rule
                    // in the rules.json file
                    String subFieldForRule = subFieldsToApplyToRule.getString(j);
                    dataField.getSubfields().forEach(subField -> {
                      String data = subField.getData();
                      char sub = subField.getCode();
                      if (sub == subFieldForRule.toCharArray()[0]) {
                        if (sb.length() > 0) {
                          sb.append(" ");
                        }
                        // remove \ char if it is the last char of the text
                        if (data.endsWith("\\")) {
                          data = data.substring(0, data.length() - 1);
                        }
                        // replace our delmiter | with ' ' and escape " with \\"
                        data = data.replace('|', ' ');// .replace("\\\\", "");
                        sb.append(removeEscapedChars(data).replaceAll("\\\"", "\\\\\""));
                      }
                    });
                  }
                  String embeddedFields[] = subFieldRule.getString("target").split("\\.");
                  if (!isMappingValid(object, embeddedFields)) {
                    log.debug("bad mapping " + subFieldRule.encode());
                    continue;
                  }
                  Object val = getValue(object, embeddedFields, sb.toString());
                  buildObject(object, embeddedFields, createNewComplexObj, val, rememberComplexObj);
                  createNewComplexObj = false;
                  ((Instance)object).setId(UUID.randomUUID().toString());
                }
              }
            }
            String res = managePushToDB(importSQLStatement, tenantId, object, false, okapiHeaders);
            if(res != null){
              block.fail(new Exception(res));
              log.error(res);
              asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
                PostLoadMarcDataResponse.withPlainInternalServerError("stopped while processing first " + processedCount +
                  " records. " + res)));
              return;
            }
          }
          String res = managePushToDB(importSQLStatement, tenantId, object, true, okapiHeaders);
          if(res != null){
            block.fail(new Exception(res));
            log.error(res);
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
              PostLoadMarcDataResponse.withPlainInternalServerError("stopped while processing first " + processedCount +
                " records. " + res)));
            return;
          }
          System.out.println(processedCount);
          long end = System.currentTimeMillis();
          log.info("inserted " + processedCount + " in " + (end - start)/1000 + " seconds" );
          block.complete("Received count: " + processedCount + "\nerrors: " + unprocessed.toString());
        }
        catch(Exception e){
          block.fail(e);
          log.error(e);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            PostLoadMarcDataResponse.withPlainInternalServerError("stopped while processing record #" + processedCount +
              ". " + e.getMessage())));
          return;
        }
        finally {
          if (entity != null) {
            try {
              entity.close();
            } catch (IOException e) {
              e.printStackTrace();
            }
          }
          client.closeClient();
        }
        }, false, whenDone -> {
          if(whenDone.succeeded()){
            asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
              PostLoadMarcDataResponse.withCreated(whenDone.result().toString())));
            return;
          }
        });
      }
    });
  }

  private String managePushToDB(StringBuffer importSQLStatement, String tenantId, Object record, boolean done, Map<String, String> okapiHeaders) throws Exception {
    if (importSQLStatement.length() == 0) {
      importSQLStatement.append("COPY " + tenantId
          + "_mod_inventory_storage.instance(_id,jsonb) FROM STDIN  DELIMITER '|' ENCODING 'UTF8';");
      importSQLStatement.append(System.lineSeparator());
    }
    importSQLStatement.append(((Instance)record).getId()).append("|").append(ObjectMapperTool.getMapper().writeValueAsString(record)).append(
      System.lineSeparator());
    counter++;
    if (counter == (bulkSize+1) || done) {
      counter = 0;
      importSQLStatement.append("\\.");
/*
      HttpClient httpclient = new DefaultHttpClient();
      HttpPost post = new HttpPost(url + IMPORT_URL);
      HttpEntity entity = new HttpEntity();

      entity.writeTo(new OutputStreamWriter(out, enc));
      post.setEntity(entity);
      // Execute the request
      HttpResponse response = httpclient.execute(post);
      // Examine the response status
      System.out.println(response.getStatusLine());

      // Get hold of the response entity
      HttpEntity entity = response.getEntity();

      httpclient.getConnectionManager().shutdown();
      HttpGet request = new HttpGet(url);

      Map<String, String> headers = new HashMap<>();
      headers.put("Content-type", "application/octet-stream");
      headers.put("Accept", "text/plain");
      CompletableFuture<org.folio.rest.tools.client.Response> resp = client.request(HttpMethod.POST,
        Buffer.buffer(importSQLStatement.toString(), "UTF8"), IMPORT_URL, headers);
      //since we are in a separate thread anyways, we can use get() without blocking the vertx loop
      org.folio.rest.tools.client.Response response = resp.get();*/
      HttpResponse response = post(url + IMPORT_URL , importSQLStatement, okapiHeaders);
      importSQLStatement.delete(0, importSQLStatement.length());
      if (response.getStatusLine().getStatusCode() != 200) {
        String e = IOUtils.toString( response.getEntity().getContent() , "UTF8");
        log.error(e);
        return e;
      }
      //ok
      return null;
    }
    return null;
  }

  private HttpResponse post(String url, StringBuffer data, Map<String, String> okapiHeaders)
      throws ClientProtocolException, IOException {

    HttpClient httpclient = new DefaultHttpClient();
    HttpPost httpPost = new HttpPost(url);
    StringEntity entity = new StringEntity(data.toString(), "UTF8");
    httpPost.setEntity(entity);
    httpPost.setHeader(RestVerticle.OKAPI_HEADER_TENANT, okapiHeaders.get(RestVerticle.OKAPI_HEADER_TENANT));
    httpPost.setHeader(RestVerticle.OKAPI_HEADER_TOKEN, okapiHeaders.get(RestVerticle.OKAPI_HEADER_TOKEN));
    httpPost.setHeader(RestVerticle.OKAPI_USERID_HEADER, okapiHeaders.get(RestVerticle.OKAPI_USERID_HEADER));
    httpPost.setHeader("Content-type", "application/octet-stream");
    httpPost.setHeader("Accept", "text/plain");
    // Execute the request
    HttpResponse response = httpclient.execute(httpPost);
    // Examine the response status
    entity = null;
    return response;
    }

  public static String rebuildPath(Object object, String[] path, int loc) {
    StringBuffer sb = new StringBuffer();
    Class<?> type = null;
    for (int j = 0; j < path.length; j++) {
      // plain entry, not an object
      try {
        Field field = object.getClass().getDeclaredField(path[j]);
        type = field.getType();

        if (type.isAssignableFrom(java.util.List.class)
            || type.isAssignableFrom(java.util.Set.class)) {
          ParameterizedType listType = (ParameterizedType) field.getGenericType();
          Class<?> listTypeClass = (Class<?>) listType.getActualTypeArguments()[0];
          object = listTypeClass.newInstance();
          sb.append(path[j]).append("[").append(loc).append("]");
        } else {
          sb.append(path[j]);
        }
        if (!(j == path.length - 1)) {
          sb.append(".");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return sb.toString();
  }

  public static String buildObject(Object object, String[] path, boolean newComp, Object val,
      Object[] complexPreviouslyCreated) {
    Object instance = object;
    Class<?> type = null;
    for (int j = 0; j < path.length; j++) {
      // plain entry, not an object
      try {
        Field field = object.getClass().getDeclaredField(path[j]);
        type = field.getType();

        if (type.isAssignableFrom(java.util.List.class)
            || type.isAssignableFrom(java.util.Set.class)) {
          Method method = object.getClass().getMethod(columnNametoCamelCaseWithget(path[j]));
          Collection<Object> coll = ((Collection<Object>) method.invoke(object));
          int size = coll.size();
          ParameterizedType listType = (ParameterizedType) field.getGenericType();
          Class<?> listTypeClass = (Class<?>) listType.getActualTypeArguments()[0];
          if (isPrimitiveOrPrimitiveWrapperOrString(listTypeClass)) {
            coll.add(val);
          } else {
            if (newComp) {
              // create a new instance
              Object o = listTypeClass.newInstance();
              coll.add(o);
              object.getClass().getMethod(columnNametoCamelCaseWithset(path[j]), type).invoke(
                object, coll);
              object = o;
              complexPreviouslyCreated[0] = o;
            } else if (complexPreviouslyCreated[0] != null) {
              if (complexPreviouslyCreated[0].getClass().isAssignableFrom(listTypeClass)) {
                object = complexPreviouslyCreated[0]; // .getClass().getMethod(columnNametoCamelCaseWithset(path[j]) , type);
              }
            }
          }
        } else if (!isPrimitiveOrPrimitiveWrapperOrString(type)) {
          Method method = object.getClass().getMethod(columnNametoCamelCaseWithget(path[j]));
          object = method.invoke(object);
          if (object == null) {
            object = object.getClass().getMethod(columnNametoCamelCaseWithset(path[j]),
              type).invoke(object, type.newInstance());
          }
        } else {
          // primitive
          object.getClass().getMethod(columnNametoCamelCaseWithset(path[j]),
            new Class[] { val.getClass() }).invoke(object, val);
        }
        if (!(j == path.length - 1)) {
          // sb.append(".");
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return null;// sb.toString();
  }

  public static Object getValue(Object object, String[] path, String value) {
    Class<?> type = null;
    for (int j = 0; j < path.length; j++) {
      // plain entry, not an object
      try {
        Field field = object.getClass().getDeclaredField(path[j]);
        type = field.getType();
        if (type.isAssignableFrom(java.util.List.class)
            || type.isAssignableFrom(java.util.Set.class)) {
          ParameterizedType listType = (ParameterizedType) field.getGenericType();
          type = (Class<?>) listType.getActualTypeArguments()[0];
          object = type.newInstance();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return getValue(type, value);
  }

  private static Object getValue(Class<?> type, String value) {
    Object val = null;
    if (type.isAssignableFrom(String.class)) {
      val = value;
    } else if (type.isAssignableFrom(Boolean.class)) {
      val = Boolean.valueOf(value);
    } else if (type.isAssignableFrom(Double.class)) {
      val = Double.valueOf(value);
    } else {
      val = Integer.valueOf(value);
    }
    return val;
  }

  public static boolean isMappingValid(Object object, String[] path)
      throws InstantiationException, IllegalAccessException {
    Class<?> type = null;
    for (int i = 0; i < path.length; i++) {
      Field field = null;
      try {
        field = object.getClass().getDeclaredField(path[i]);
      } catch (NoSuchFieldException e) {
        // e.printStackTrace();
        return false;
      }
      type = field.getType();
      // this is a configuration error, the type is an object, but no fields are indicated
      // to be populated on that object. if you map a marc field to an object, it must be
      // something like - marc.identifier -> identifierObject.idField
      // if(!isPrimitiveOrPrimitiveWrapperOrString( type ) && path.length == 1){
      if (type.isAssignableFrom(java.util.List.class)
          || type.isAssignableFrom(java.util.Set.class)) {
        ParameterizedType listType = (ParameterizedType) field.getGenericType();
        Class<?> listTypeClass = (Class<?>) listType.getActualTypeArguments()[0];
        object = listTypeClass.newInstance();
        if (isPrimitiveOrPrimitiveWrapperOrString(listTypeClass) && i == path.length - 1) {
          // we are here if the last entry in the path is an array / set of primitives, that is ok
          return true;
        }
      }
      /*
       * else { return false; }
       */
      // }
    }
    if (!isPrimitiveOrPrimitiveWrapperOrString(type)) {
      return false;
    }
    return true;
  }

  public static boolean isPrimitiveOrPrimitiveWrapperOrString(Class<?> type) {
    return (type.isPrimitive() && type != void.class) || type == Double.class || type == Float.class
        || type == Long.class || type == Integer.class || type == Short.class
        || type == Character.class || type == Byte.class || type == Boolean.class
        || type == String.class;
  }

  private static String columnNametoCamelCaseWithset(String str) {
    StringBuilder sb = new StringBuilder(str);
    sb.replace(0, 1, String.valueOf(Character.toUpperCase(sb.charAt(0))));
    for (int i = 0; i < sb.length(); i++) {
      if (sb.charAt(i) == '_') {
        sb.deleteCharAt(i);
        sb.replace(i, i + 1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
      }
    }
    return "set" + sb.toString();
  }

  private static String columnNametoCamelCaseWithget(String str) {
    StringBuilder sb = new StringBuilder(str);
    sb.replace(0, 1, String.valueOf(Character.toUpperCase(sb.charAt(0))));
    for (int i = 0; i < sb.length(); i++) {
      if (sb.charAt(i) == '_') {
        sb.deleteCharAt(i);
        sb.replace(i, i + 1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
      }
    }
    return "get" + sb.toString();
  }

  private static String removeEscapedChars(String path) {
    int len = path.length();
    List<String> res = new ArrayList<>();
    StringBuilder token = new StringBuilder();
    boolean slash = false;
    boolean isEven = false;

    for (int j = 0; j < len; j++) {
      char t = path.charAt(j);
      if (slash && isEven && t == '\\') {
        // we've seen \\ and now a third \ in a row
        isEven = false;
        slash = true;
        token.append(t);
      } else if (slash && !isEven && t == '\\') {
        // we have seen an odd number of \ and now we see another one, meaning \\ in the string
        slash = true;
        isEven = true;
        token.append(t);
      } else if (slash && !isEven && t != '\\') {
        // we've hit a non \ after a single \, this needs to get encoded to be \\
        token.append('\\').append(t);
        isEven = false;
        slash = false;
      } else if (!slash && t == '\\') {
        // we've hit a \
        token.append(t);
        isEven = false;
        slash = true;
      } else {
        // even number of slashes following by a non slash, or just a non slash
        token.append(t);
        isEven = false;
        slash = false;
      }
    }
    return token.toString();
  }

}
