package org.folio.rest.impl;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
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
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Leader;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.impl.SubfieldImpl;

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
  private ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
  private Map<Integer, CompiledScript> preCompiledJS = new HashMap<>();

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

  @Override
  public void postLoadMarcDataTest(InputStream entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) throws Exception {

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if(validRequest(asyncResultHandler, okapiHeaders)){
      process(true, entity, vertxContext, tenantId, asyncResultHandler, okapiHeaders);
    }
  }

  @Validate
  @Override
  public void postLoadMarcData(String storageURL, int bulkSize, InputStream entity,
      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) throws Exception {

    long start = System.currentTimeMillis();

    if(!validRequest(asyncResultHandler, okapiHeaders)){
      return;
    }

    this.bulkSize = bulkSize;

    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

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
        process(false, entity, vertxContext, tenantId, asyncResultHandler, okapiHeaders);
      }
    });
  }


  private boolean validRequest(Handler<AsyncResult<Response>> asyncResultHandler, Map<String, String> okapiHeaders){
    String tenantId = TenantTool.calculateTenantId(
      okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));

    if (tenantId == null) {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest("tenant not set")));
      return false;
    }

    JsonObject rulesFile = tenantRulesMap.get(tenantId);

    if(rulesFile == null){
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest("no rules file found for tenant " + tenantId)));
      return false;
    }

    return true;
  }

  private void process(boolean isTest, InputStream entity, Context vertxContext, String tenantId,
      Handler<AsyncResult<Response>> asyncResultHandler, Map<String, String> okapiHeaders){

    long start = System.currentTimeMillis();

    long jsPerfTime[] = new long[]{0};
    long jsPerfCount[] = new long[]{0};

    JsonObject rulesFile = tenantRulesMap.get(tenantId);

    if(rulesFile == null){
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
        PostLoadMarcDataResponse.withPlainBadRequest("no rules file found for tenant " + tenantId)));
      return;
    }

    vertxContext.owner().executeBlocking( block -> {
      log.info("REQUEST ID " + UUID.randomUUID().toString());
      try {
      final MarcStreamReader reader = new MarcStreamReader(entity);
      StringBuffer unprocessed = new StringBuffer();
      Object object = new Instance();
      while (reader.hasNext()) {
        processedCount++;
        List<DataField> df = null;
        List<ControlField> cf = null;
        Leader leader[] = new Leader[]{null};
        try {
          Record record = reader.next();
          df = record.getDataFields();
          cf = record.getControlFields();
          leader[0] = record.getLeader();
        } catch (Exception e) {
          unprocessed.append("#").append(processedCount).append(" ");
          log.error(e.getMessage(), e);
          continue;
        }
        Iterator<ControlField> ctrlIter = cf.iterator();
        Iterator<DataField> iter = df.iterator();
        object = new Instance();
        while (ctrlIter.hasNext()) {
          //no subfields
          ControlField controlField = ctrlIter.next();
          //get entry for this control field in the rules.json file
          JsonArray entriesForControlField = rulesFile.getJsonArray(controlField.getTag());
          //when populating an object with multiple fields from the same marc field
          //this is used to pass the reference of the previously created object to the buildObject function
          Object rememberComplexObj[] = new Object[] { null };
          boolean createNewComplexObj = true;
          if (entriesForControlField != null) {
            for (int i = 0; i < entriesForControlField.size(); i++) {
              JsonObject entryForControlField = entriesForControlField.getJsonObject(i);
              //get rules - each rule can contain multiple conditions that need to be met and a
              //value to inject in case all the conditions are met
              JsonArray rules = entryForControlField.getJsonArray("rules");
              //the content of the control field
              String data = controlField.getData();
              if(rules != null){
                data = processRules(data, rules, leader[0]);
              }
              if(data != null){
                data = removeEscapedChars(data).replaceAll("\\\"", "\\\\\"");
                if(data.length() == 0){
                  continue;
                }
              }
              //if conditionsMet = true, then all conditions of a specific rule were met
              //and we can set the target to the rule's value
              String target = entryForControlField.getString("target");
              String embeddedFields[] = target.split("\\.");
              if (!isMappingValid(object, embeddedFields)) {
                log.debug("bad mapping " + rules.encode());
                continue;
              }
              Object val = getValue(object, embeddedFields, data);
              buildObject(object, embeddedFields, createNewComplexObj, val, rememberComplexObj);
              createNewComplexObj = false;
            }
          }
        }
        while (iter.hasNext()) {
          // this is an iterator on the marc record, field by field
          boolean createNewComplexObj = true; // each rule will generate a new object in an array , for an array data member
          Object rememberComplexObj[] = new Object[] { null };
          DataField dataField = iter.next();
          JsonArray mappingEntry = rulesFile.getJsonArray(dataField.getTag());
          if (mappingEntry != null) {
            //there is a mapping associated with this marc field
            for (int i = 0; i < mappingEntry.size(); i++) {
              //there could be multiple mapping entries, specifically different mappings
              //per subfield in the marc field
              JsonObject subFieldMapping = mappingEntry.getJsonObject(i);
              //a single mapping entry can also map multiple subfields to a specific field in
              //the instance
              JsonArray instanceField = subFieldMapping.getJsonArray("entity");
              //entity field indicates that the subfields within the entity definition should be
              //a single object, anything outside the entity definition will be placed in another
              //object of the same type, unless the target points to a different type.
              //multiple entities can be declared in a field, meaning each entity will be a new object
              //with the subfields defined in a single entity grouped as a single object.
              //all definitions not enclosed within the entity will be associated with anothe single object
              boolean entityRequested = false;
              //for repeatable subfields, you can indicate that each repeated subfield should respect
              //the new object declaration and create a new object. so that if there are two "a" subfields
              //each one will create its own object
              Boolean entityRequestedPerRepeatedSubfield = subFieldMapping.getBoolean("entityPerRepeatedSubfield");
              if(entityRequestedPerRepeatedSubfield == null){
                entityRequestedPerRepeatedSubfield = false;
              }
              else{
                entityRequestedPerRepeatedSubfield = true;
              }
              //if no "entity" is defined , then the contents of the field getting mapped to the same type
              //will be placed in a single object of that type.
              if(instanceField == null){
                instanceField = new JsonArray();
                instanceField.add(subFieldMapping);
              }
              else{
                entityRequested = true;
              }
              List<Object[]> obj = new ArrayList<>();
              for (int z = 0; z < instanceField.size(); z++) {
                JsonObject jObj = instanceField.getJsonObject(z);
                JsonArray subFields = jObj.getJsonArray("subfield");
                //push into a set so that we can do a lookup for each subfield in the marc instead
                //of looping over the array
                Set<String> subFieldsSet = new HashSet<String>(subFields.getList());
                //it can be a one to one mapping, or there could be rules to apply prior to the mapping
                JsonArray rules = jObj.getJsonArray("rules");
                //allow to declare a delimiter when concatenating subfields.
                //also allow , in a multi subfield field, to have some subfields with delimiter x and
                //some with delimiter y, and include a separator to separate each set of subfields
                //maintain a delimiter per subfield map - to lookup the correct delimiter and place it in string
                //maintain a string buffer per subfield - but with the string buffer being a reference to the
                //same stringbuffer for subfields with the same delimiter - the stringbuffer references are
                //maintained in the buffers2concat list which is then iterated over and we place a separator
                //between the content of each string buffer reference's content
                JsonArray delimiters = jObj.getJsonArray("subFieldDelimiter");
                //this is a map of each subfield to the delimiter to delimit it with
                final Map<String, String> subField2Delimiter = new HashMap<>();
                //map a subfield to a stringbuffer which will hold its content
                //since subfields can be concatenated into the same stringbuffer
                //the map of different subfields can map to the same stringbuffer reference
                final Map<String, StringBuffer> subField2Data = new HashMap<>();
                //keeps a reference to the stringbuffers that contain the data of the
                //subfield sets. this list is then iterated over and used to delimit subfield sets
                final List<StringBuffer> buffers2concat = new ArrayList<>();
                //separator between subfields with different delimiters
                String separator[] = new String[]{ null };
                if(delimiters != null){
                  for (int j = 0; j < delimiters.size(); j++) {
                    JsonObject job = delimiters.getJsonObject(j);
                    String delimiter = job.getString("value");
                    JsonArray subFieldswithDel = job.getJsonArray("subfields");
                    StringBuffer subFieldsStringBuffer = new StringBuffer();
                    buffers2concat.add(subFieldsStringBuffer);
                    if(subFieldswithDel.size() == 0){
                      separator[0] = delimiter;
                    }
                    for (int k = 0; k < subFieldswithDel.size(); k++) {
                      subField2Delimiter.put(subFieldswithDel.getString(k), delimiter);
                      subField2Data.put(subFieldswithDel.getString(k), subFieldsStringBuffer);
                    }
                  }
                }
                else{
                  buffers2concat.add(new StringBuffer());
                }
                String embeddedFields[] = jObj.getString("target").split("\\.");
                if (!isMappingValid(object, embeddedFields)) {
                  log.debug("bad mapping " + jObj.encode());
                  continue;
                }
                //iterate over the subfields in the mapping entry
                List<Subfield> subs = dataField.getSubfields();
                //check if we need to expand the subfields into additional subfields
                JsonObject splitter = jObj.getJsonObject("subFieldSplit");
                if(splitter != null){
                  expandSubfields(subs, splitter);
                }
                int size = subs.size();
                for (int k = 0; k < size; k++) {
                  String data = subs.get(k).getData();
                  char sub1 = subs.get(k).getCode();
                  String subfield = String.valueOf(sub1);
                  if (subFieldsSet.contains(subfield)) {
                    //rule file contains a rule for this subfield
                    if(obj.size() <= k){
                      //temporarily save objects with multiple fields so that the fields of the
                      //same object can be populated with data from different subfields
                      for (int l = obj.size(); l <= k; l++) {
                        obj.add(new Object[] { null });
                      }
                    }
                    if(rules != null){
                      data = processRules(data, rules, leader[0]);
                    }
                    if(delimiters != null){
                      //delimiters is not null, meaning we have a string buffer for each set of subfields
                      //so populate the appropriate string buffer
                      if (subField2Data.get(String.valueOf(subfield)).length() > 0) {
                        subField2Data.get(String.valueOf(subfield)).append(subField2Delimiter.get(subfield));
                      }
                    }
                    // remove \ char if it is the last char of the text
                    if (data.endsWith("\\")) {
                      data = data.substring(0, data.length() - 1);
                    }
                    data = removeEscapedChars(data).replaceAll("\\\"", "\\\\\"");
                    if(delimiters != null){
                      subField2Data.get(subfield).append(data);
                    }
                    else{
                      StringBuffer sb = buffers2concat.get(0);
                      if(entityRequestedPerRepeatedSubfield){
                        //create a new value no matter what , since this use case
                        //indicates that repeated and non-repeated subfields will create a new entity
                        //so we should not concat values
                        sb.delete(0, sb.length());
                      }
                      if(sb.length() > 0){
                        sb.append(" ");
                      }
                      sb.append(data);
                    }
                    if(entityRequestedPerRepeatedSubfield && entityRequested){
                      if(obj.get(k)[0] != null){
                        createNewComplexObj = false;
                      }
                      else{
                        createNewComplexObj = true;
                      }
                      createNewObject(embeddedFields, object, buffers2concat, separator[0], createNewComplexObj, obj.get(k));
                    }
                  }
                }
                if(!(entityRequestedPerRepeatedSubfield && entityRequested)){
                  boolean created =
                      createNewObject(embeddedFields, object, buffers2concat, separator[0], createNewComplexObj, rememberComplexObj);
                  if(created){
                    createNewComplexObj = false;
                  }
                }
                ((Instance)object).setId(UUID.randomUUID().toString());
              }
              if(entityRequested){
                createNewComplexObj = true;
              }
            }
          }
        }
        String res = managePushToDB(isTest, importSQLStatement, tenantId, object, false, okapiHeaders);
        if(res != null){
          block.fail(new Exception(res));
          log.error(res);
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            PostLoadMarcDataResponse.withPlainInternalServerError("stopped while processing first " + processedCount +
              " records. " + res)));
          return;
        }
      }
      String res = managePushToDB(isTest, importSQLStatement, tenantId, null, true, okapiHeaders);
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
      log.error(e.getMessage(), e);
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
          log.error(e.getMessage(), e);
        }
      }
      if(client != null){
        client.closeClient();
      }
    }
    }, false, whenDone -> {
      if(whenDone.succeeded()){
        if(isTest){
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            PostLoadMarcDataTestResponse.withPlainCreated(importSQLStatement.toString())));
        }else{
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            PostLoadMarcDataResponse.withCreated(whenDone.result().toString())));
        }
        return;
      }
    });
  }

  private boolean createNewObject(String embeddedFields[], Object object,
      List<StringBuffer> buffers2concat, String separator, boolean createNewComplexObj, Object rememberComplexObj[]) throws Exception {
    StringBuffer finalData = new StringBuffer();
    int size = buffers2concat.size();
    for(int x=0; x<size; x++){
      StringBuffer sb = buffers2concat.get(x);
      if(sb.length() > 0){
        if(finalData.length() > 0){
          finalData.append(separator);
        }
        finalData.append(sb);
      }
    }
    if(finalData.length() != 0){
      Object val = getValue(object, embeddedFields, finalData.toString());
      try {
        return buildObject(object, embeddedFields, createNewComplexObj, val, rememberComplexObj);
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        return false;
      }
    }
    return false;
  }

  /**
   * replace the existing subfields in the datafield with subfields generated on the data of the subfield
   * for example: $aitaspa in 041 would be the language of the record. this can be split into two $a subfields
   * $aita and $aspa so that it can be concatenated properly or even become two separate fields with the
   * entity per repeated subfield flag
   * the data is expanded by the implementing function (can be custom as well) - the implementing function
   * receives data from ONE subfield at a time - two $a subfields will be processed separately.
   * @param subs
   * @param splitConf
   * @throws ScriptException
   */
  private void expandSubfields(List<Subfield> subs, JsonObject splitConf) throws ScriptException {
    List<Subfield> expandedSubs = new ArrayList<>();
    String func = splitConf.getString("type");
    boolean isCustom = false;
    if("custom".equals(func)){
      isCustom = true;
    }
    String param = splitConf.getString("value");
    int size = subs.size();
    for (int k = 0; k < size; k++) {
      String data = subs.get(k).getData();
      Iterator<?> splitData = null;
      if(isCustom){
        splitData = ((jdk.nashorn.api.scripting.ScriptObjectMirror)runJScript(param, data)).values().iterator();
      }
      else{
        splitData = NormalizationFunctions.runSplitFunction(func, data, param);
      }
      while (splitData.hasNext()) {
        String newData = (String)splitData.next();
        Subfield sub = new SubfieldImpl(subs.get(k).getCode(), newData);
        expandedSubs.add(sub);
      }
    }
    subs.clear();
    subs.addAll( expandedSubs );
  }

  private String processRules(String data, JsonArray rules, Leader leader){
    //there are rules associated with this subfield to instance field mapping
    String originalData = data;
    for (int k = 0; k < rules.size(); k++) {
      //get the rules one by one
      JsonObject rule = rules.getJsonObject(k);
      //get the conditions associated with each rule
      JsonArray conditions = rule.getJsonArray("conditions");
      //get the constant value to set the instance field to in case all
      //conditions are met for a rule, since there can be multiple rules
      //each with multiple conditions, a match of all conditions in a single rule
      //will set the instance's field to the const value. hence, it is an AND
      //between all conditions and an OR between all rules
      String ruleConstVal = rule.getString("value");
      boolean conditionsMet = true;
      //each rule has conditions, if they are all met, then mark
      //continue processing the next condition, if all conditions are met
      //set the target to the value of the rule
      boolean isCustom = false;
      for (int m = 0; m < conditions.size(); m++) {
        JsonObject condition = conditions.getJsonObject(m);
        //1..n functions can be declared in a condition
        //the functions here can rely on the single value field for comparison
        //to the output of all functions on the marc's field data
        //or, if a custom function is declared, the value will contain
        //the javascript of the custom function
        String []function = condition.getString("type").split(",");
        for (int n = 0; n < function.length; n++) {
          if("custom".equals(function[n].trim())){
            isCustom = true;
            break;
          }
        }
        if(leader != null && condition.getBoolean("LDR") != null){
          //the rule also has a condition on the leader field
          data = leader.toString();
        }
        String valueParam = condition.getString("value");
        for (int l = 0; l < function.length; l++) {
          if("custom".equals(function[l].trim())){
            long startJS = System.nanoTime();
            try{
              data = (String)runJScript(valueParam, data);
            }
            catch(Exception e){
              //the function has thrown an exception meaning this condition has failed,
              //hence this specific rule has failed
              conditionsMet = false;
              log.error(e.getMessage(), e);
            }
            long endtJS = System.nanoTime();
            //jsPerfTime[0] = jsPerfTime[0]+(endtJS-startJS);
            //jsPerfCount[0]++;
          }
          else{
            String c = NormalizationFunctions.runFunction(function[l].trim(), data, condition.getString("parameter"));
            if(valueParam != null && !c.equals(valueParam) && !isCustom){
              //still allow a condition to compare the output of a function on the data to a constant value
              //unless this is a custom javascript function in which case, the value holds the custom function
              conditionsMet = false;
              break;
            }
            else if (ruleConstVal == null){
              //if there is no val to use as a replacement , then assume the function
              //is doing the value update and set the data to the returned value
              data = c;
            }
          }
        }
        if(!conditionsMet){
          //all conditions for this rule we not met, revert data to the originalData passed in.
          data = originalData;
          break;
        }
      }
      if(conditionsMet && ruleConstVal != null && !isCustom){
        //all conditions of the rule were met, and there
        //is a constant value associated with the rule, and this is
        //not a custom rule, then set the data to the const value
        //no need to continue processing other rules for this subfield
        data = ruleConstVal;
        break;
      }
    }
    return data;
  }

  private Object runJScript(String jscript, String data) throws ScriptException {
    CompiledScript script = preCompiledJS.get(jscript.hashCode());
    if(script == null){
      log.debug("compiling JS function: " + jscript);
      script = ((Compilable) engine).compile(jscript);
      preCompiledJS.put(jscript.hashCode(), script);
    }
    Bindings bindings = new SimpleBindings();
    bindings.put("DATA", data);
    return script.eval(bindings);
  }

  private String managePushToDB(boolean isTest, StringBuffer importSQLStatement, String tenantId, Object record, boolean done, Map<String, String> okapiHeaders) throws Exception {
    if(importSQLStatement.length() == 0 && record == null && done) {
      //no more marcs to process, we reached the end of the loop, and we have no records in the buffer to flush to the db then just return,
      return null;
    }
    if (importSQLStatement.length() == 0 && !isTest) {
      importSQLStatement.append("COPY " + tenantId
          + "_mod_inventory_storage.instance(_id,jsonb) FROM STDIN  DELIMITER '|' ENCODING 'UTF8';");
      importSQLStatement.append(System.lineSeparator());
    }
    if(record != null){
      importSQLStatement.append(((Instance)record).getId()).append("|").append(ObjectMapperTool.getMapper().writeValueAsString(record)).append(
        System.lineSeparator());
    }
    counter++;
    if (counter == bulkSize || done) {
      counter = 0;
      if(!isTest){
        importSQLStatement.append("\\.");
        HttpResponse response = post(url + IMPORT_URL , importSQLStatement, okapiHeaders);
        importSQLStatement.delete(0, importSQLStatement.length());
        if (response.getStatusLine().getStatusCode() != 200) {
          String e = IOUtils.toString( response.getEntity().getContent() , "UTF8");
          log.error(e);
          return e;
        }
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

  public static boolean buildObject(Object object, String[] path, boolean newComp, Object val,
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
      } catch (Exception e) {
        log.error(e.getMessage(), e);
        return false;
      }
    }
    return true;// sb.toString();
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
      if( t == '|'){
        t = ' ';
      }
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
