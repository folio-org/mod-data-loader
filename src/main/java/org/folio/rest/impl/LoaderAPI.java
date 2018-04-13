package org.folio.rest.impl;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.resource.LoadResource;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.OutStream;
import org.folio.rest.tools.utils.TenantTool;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

import static org.folio.rest.service.LoaderHelper.isPrimitiveOrPrimitiveWrapperOrString;


public class LoaderAPI implements LoadResource {

  private static final Logger LOGGER = LogManager.getLogger(LoaderAPI.class);
  private static final String TENANT_ID_NULL = TenantTool.calculateTenantId(null);
  private static final String TENANT_NOT_SET = "tenant not set";
  private static final String NOT_IMPLEMENTED = "Not implemented";

  // rules are not stored in db as this is a test loading module
  static final Map<String, JsonObject> TENANT_RULES_MAP = new HashMap<>();
  private int bulkSize = 50000;
  private Processor processor = new Processor();

  @Override
  public void postLoadMarcRules(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if (tenantId.equals(TENANT_ID_NULL)) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcRulesResponse.withPlainBadRequest(TENANT_NOT_SET)));
      return;
    }
    String sqlFile = IOUtils.toString(entity, "UTF8");

    try {
      TENANT_RULES_MAP.put(tenantId, new JsonObject(sqlFile));
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

    if (tenantId.equals(TENANT_ID_NULL)) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcRulesResponse.withPlainBadRequest(TENANT_NOT_SET)));
      return;
    }

    OutStream stream = new OutStream();
    stream.setData(TENANT_RULES_MAP.get(tenantId));
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetLoadMarcRulesResponse.withJsonOK(stream)));
  }

  @Override
  public void getLoadMarcData(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      GetLoadMarcDataResponse.withPlainMethodNotAllowed(NOT_IMPLEMENTED)));
  }

  @Override
  public void postLoadMarcDataTest(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if(validRequest(asyncResultHandler, okapiHeaders)){
      processor.process(true, entity, vertxContext, tenantId, asyncResultHandler, okapiHeaders, bulkSize);
    }
  }

  @Validate
  @Override
  public void postLoadMarcData(String storageURL, int bulkSize, InputStream entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    if(!validRequest(asyncResultHandler, okapiHeaders)){
      return;
    }

    this.bulkSize = bulkSize;
    String tenantId = TenantTool.calculateTenantId(okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));
    processor.setUrl(storageURL);
    HttpClientInterface client = HttpClientFactory.getHttpClient(storageURL, tenantId);

    //check if inventory storage is responding
    Map<String, String> headers = new HashMap<>();
    headers.put("Accept", "text/plain");
    CompletableFuture<org.folio.rest.tools.client.Response> resp = client.request( "/admin/health" , headers );
    resp.whenComplete( (response, error) -> {
      try {
        if(error != null){
          LOGGER.error(error.getCause());
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            PostLoadMarcDataResponse.withPlainBadRequest("Unable to connect to the inventory storage module..." + error.getMessage())));
          return;
        }
        if(response.getCode() != 200){
          LOGGER.error("Unable to connect to the inventory storage module at..." + storageURL);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            PostLoadMarcDataResponse.withPlainBadRequest("Unable to connect to the inventory storage module at..." + storageURL)));
        }
        else{
          processor.process(false, entity, vertxContext, tenantId, asyncResultHandler, okapiHeaders, bulkSize);
        }
      } finally {
        client.closeClient();
      }
    });
  }

  private boolean validRequest(Handler<AsyncResult<Response>> asyncResultHandler, Map<String, String> okapiHeaders){
    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if (tenantId.equals(TENANT_ID_NULL)) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest(TENANT_NOT_SET)));
      return false;
    }

    JsonObject rulesFile = TENANT_RULES_MAP.get(tenantId);

    if(rulesFile == null){
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest("no rules file found for tenant " + tenantId)));
      return false;
    }

    return true;
  }

  /**
   *
   * @param object - the root object to start parsing the 'path' from
   * @param path - the target path - the field to place the value in
   * @param newComp - should a new object be created , if not, use the object passed into the
   * complexPreviouslyCreated parameter and continue populating it.
   * @param val
   * @param complexPreviouslyCreated - pass in a non primitive pojo that is already partially
   * populated from previous subfield values
   * @return
   */
  static boolean buildObject(Object object, String[] path, boolean newComp, Object val,
                             Object[] complexPreviouslyCreated) {
    Class<?> type;
    for (String pathSegment : path) {
      try {
        Field field = object.getClass().getDeclaredField(pathSegment);
        type = field.getType();
        if (type.isAssignableFrom(List.class) || type.isAssignableFrom(java.util.Set.class)) {

          Method method = object.getClass().getMethod(columnNametoCamelCaseWithget(pathSegment));
          Collection<Object> coll = setColl(method, object);
          ParameterizedType listType = (ParameterizedType) field.getGenericType();
          Class<?> listTypeClass = (Class<?>) listType.getActualTypeArguments()[0];
          if (isPrimitiveOrPrimitiveWrapperOrString(listTypeClass)) {
            coll.add(val);
          } else {
            object = setObjectCorrectly(newComp, listTypeClass, type, pathSegment, coll, object, complexPreviouslyCreated[0]);
            complexPreviouslyCreated[0] = object;
          }
        } else if (!isPrimitiveOrPrimitiveWrapperOrString(type)) {
          //currently not needed for instances, may be needed in the future
          //non primitive member in instance object but represented as a list or set of non
          //primitive objects
          Method method = object.getClass().getMethod(columnNametoCamelCaseWithget(pathSegment));
          object = method.invoke(object);
        } else { // primitive
          object.getClass().getMethod(columnNametoCamelCaseWithset(pathSegment),
            val.getClass()).invoke(object, val);
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        return false;
      }
    }
    return true;
  }

  private static Object setObjectCorrectly(boolean newComp, Class<?> listTypeClass, Class<?> type, String pathSegment,
                                           Collection<Object> coll, Object object, Object complexPreviouslyCreated)
    throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {

    if (newComp) {
      Object o = listTypeClass.newInstance();
      coll.add(o);
      object.getClass().getMethod(columnNametoCamelCaseWithset(pathSegment), type).invoke(object, coll);
      return o;
    } else if ((complexPreviouslyCreated != null) &&
      (complexPreviouslyCreated.getClass().isAssignableFrom(listTypeClass))) {
      return complexPreviouslyCreated;
    }
    return object;
  }

  private static Collection<Object> setColl(Method method, Object object) throws InvocationTargetException,
    IllegalAccessException {
    return ((Collection<Object>) method.invoke(object));
  }

  static Object getValue(Object object, String[] path, String value) {
    Class<?> type = Integer.TYPE;
    for (String pathSegment : path) {
      try {
        Field field = object.getClass().getDeclaredField(pathSegment);
        type = field.getType();
        if (type.isAssignableFrom(java.util.List.class) || type.isAssignableFrom(java.util.Set.class)) {
          ParameterizedType listType = (ParameterizedType) field.getGenericType();
          type = (Class<?>) listType.getActualTypeArguments()[0];
          object = type.newInstance();
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    return getValue(type, value);
  }

  private static Object getValue(Class<?> type, String value) {
    Object val;
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

  @Validate
  @Override
  public void postLoadStatic(String storageURL, InputStream entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {

    processor.processStatic(storageURL, false, entity, okapiHeaders, asyncResultHandler, vertxContext);
  }

  @Override
  public void getLoadStatic(Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
      GetLoadStaticResponse.withPlainMethodNotAllowed(NOT_IMPLEMENTED)));
  }

  @Override
  public void postLoadStaticTest(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    processor.processStatic(null, true, entity, okapiHeaders, asyncResultHandler, vertxContext);
  }
}
