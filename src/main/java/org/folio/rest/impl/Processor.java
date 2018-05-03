package org.folio.rest.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.BooleanUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.folio.rest.RestVerticle;
import org.folio.rest.javascript.JSManager;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.resource.LoadResource;
import org.folio.rest.service.LoaderHelper;
import org.folio.rest.service.ProcessorHelper;
import org.folio.rest.struct.ProcessedSinglePlusConditionCheck;
import org.folio.rest.struct.ProcessedSingleItem;
import org.folio.rest.tools.ClientGenerator;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.rest.utils.Escaper;
import org.folio.rest.validate.JsonValidator;
import org.folio.util.IoUtil;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.*;
import org.marc4j.marc.impl.SubfieldImpl;

import javax.script.ScriptException;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.*;

import static org.folio.rest.service.LoaderHelper.isMappingValid;


class Processor {

  private static final Logger LOGGER = LogManager.getLogger(Processor.class);
  private static final String IMPORT_URL = "/admin/importSQL";
  private static final String RECORD = "record";
  private static final String VALUES = "values";
  private static final String VALUE = "value";
  private static final String CUSTOM = "custom";
  private static final String TYPE = "type";

  private int processedCount;
  private StringBuilder importSQLStatement = new StringBuilder();
  private int counter;
  private int bulkSize;
  private JsonObject rulesFile;
  private String tenantId;
  private Map<String, String> okapiHeaders;
  private String url;
  private boolean isTest;

  private Leader leader;
  private String separator; //separator between subfields with different delimiters
  private JsonArray delimiters;
  private Object object;
  private JsonArray rules;
  private boolean createNewComplexObj;
  private boolean entityRequested;
  private boolean entityRequestedPerRepeatedSubfield;
  private final List<StringBuilder> buffers2concat = new ArrayList<>();
  private final Map<String, StringBuilder> subField2Data = new HashMap<>();
  private final Map<String, String> subField2Delimiter = new HashMap<>();

  private static final int CONNECT_TIMEOUT = 3 * 1000;
  private static final int CONNECTION_TIMEOUT = 300 * 1000; //keep connection open this long
  private static final int SO_TIMEOUT = 180 * 1000; //during data flow, if interrupted for 180sec, regard connection as
  // stalled/broken.

  Processor(String tenantId, Map<String, String> okapiHeaders) {
    this.okapiHeaders = okapiHeaders;
    this.tenantId = tenantId;
    this.rulesFile = LoaderAPI.TENANT_RULES_MAP.get(tenantId);
  }

  void setUrl(String url) {
    this.url = url;
  }

  void process(boolean isTest, InputStream entity, Context vertxContext,
                       Handler<AsyncResult<Response>> asyncResultHandler, int bulkSize){

    this.isTest = isTest;
    this.bulkSize = bulkSize;
    long start = System.currentTimeMillis();

    vertxContext.owner().executeBlocking( block -> {

      LOGGER.info("REQUEST ID " + UUID.randomUUID().toString());
      try {
        final MarcStreamReader reader = new MarcStreamReader(entity);
        StringBuilder unprocessed = new StringBuilder();

        while (reader.hasNext()) {
          processSingleEntry(reader, block, unprocessed);
        }

        String error = managePushToDB(tenantId, true, okapiHeaders);
        if(error != null){
          block.fail(new Exception(error));
          return;
        }
        long end = System.currentTimeMillis();
        LOGGER.info("inserted " + processedCount + " in " + (end - start)/1000 + " seconds" );
        block.complete("Received count: " + processedCount + "\nerrors: " + unprocessed.toString());
      }
      catch(Exception e){
        block.fail(e);
      }
      finally {
        LoaderHelper.closeInputStream(entity);
      }
    }, false, whenDone -> {
      if(whenDone.succeeded()){
        if(isTest){
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            LoadResource.PostLoadMarcDataTestResponse.withPlainCreated(importSQLStatement.toString())));
        }else{
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            LoadResource.PostLoadMarcDataResponse.withCreated(whenDone.result().toString())));
        }
        LOGGER.info("Completed processing of REQUEST");
      }
      else{
        LOGGER.error(whenDone.cause().getMessage(), whenDone.cause());
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
          LoadResource.PostLoadMarcDataResponse.withPlainInternalServerError("stopped while processing record #" + processedCount +
            ". " + whenDone.cause().getMessage())));
      }
    });
  }

  private void processSingleEntry(MarcStreamReader reader, Future<Object> block, StringBuilder unprocessed) {

    try {
      processedCount++;
      List<DataField> df;
      List<ControlField> cf;
      Record record = reader.next();
      df = record.getDataFields();
      cf = record.getControlFields();
      leader = record.getLeader();
      Iterator<ControlField> ctrlIter = cf.iterator();
      Iterator<DataField> dfIter = df.iterator();
      object = new Instance();
      processMarcControlSection(ctrlIter, rulesFile);

      while (dfIter.hasNext()) {
        handleMarcRecordFieldByField(dfIter);
      }

      String error = managePushToDB(tenantId, false, okapiHeaders);
      if(error != null){
        block.fail(new Exception(error));
      }
    } catch (Exception e) {
      unprocessed.append("#").append(processedCount).append(" ");
      LOGGER.error(e.getMessage(), e);
    }
  }

  private void handleMarcRecordFieldByField(Iterator<DataField> dfIter)
    throws ScriptException, IllegalAccessException, InstantiationException {

    createNewComplexObj = true; // each rule will generate a new object in an array , for an array data member
    Object[] rememberComplexObj = new Object[] { null };
    DataField dataField = dfIter.next();
    JsonArray mappingEntry = rulesFile.getJsonArray(dataField.getTag());
    if (mappingEntry == null) {
      return;
    }

    //there is a mapping associated with this marc field
    for (int i = 0; i < mappingEntry.size(); i++) {

      //there could be multiple mapping entries, specifically different mappings
      //per subfield in the marc field
      JsonObject subFieldMapping = mappingEntry.getJsonObject(i);
      processSubFieldMapping(subFieldMapping, rememberComplexObj, dataField);
    }
  }

  private void processSubFieldMapping(JsonObject subFieldMapping, Object[] rememberComplexObj, DataField dataField)
    throws IllegalAccessException, InstantiationException, ScriptException {

    //a single mapping entry can also map multiple subfields to a specific field in the instance
    JsonArray instanceField = subFieldMapping.getJsonArray("entity");

    //entity field indicates that the subfields within the entity definition should be
    //a single object, anything outside the entity definition will be placed in another
    //object of the same type, unless the target points to a different type.
    //multiple entities can be declared in a field, meaning each entity will be a new object
    //with the subfields defined in a single entity grouped as a single object.
    //all definitions not enclosed within the entity will be associated with anothe single object
    entityRequested = false;

    //for repeatable subfields, you can indicate that each repeated subfield should respect
    //the new object declaration and create a new object. so that if there are two "a" subfields
    //each one will create its own object
    entityRequestedPerRepeatedSubfield =
      BooleanUtils.isTrue(subFieldMapping.getBoolean("entityPerRepeatedSubfield"));

    //if no "entity" is defined , then all rules contents of the field getting mapped to the same type
    //will be placed in a single object of that type.
    if(instanceField == null){
      instanceField = new JsonArray();
      instanceField.add(subFieldMapping);
    }
    else{
      entityRequested = true;
    }
    List<Object[]> arraysOfObjects = new ArrayList<>();
    for (int instanceFieldIndex = 0; instanceFieldIndex < instanceField.size(); instanceFieldIndex++) {

      handleInstanceFields(instanceField, instanceFieldIndex, arraysOfObjects, dataField, rememberComplexObj);

    }
    if(entityRequested){
      createNewComplexObj = true;
    }
  }

  private void handleInstanceFields(JsonArray instanceField, int instanceFieldIndex, List<Object[]> arraysOfObjects,
                                    DataField dataField, Object[] rememberComplexObj)
    throws ScriptException, IllegalAccessException, InstantiationException {

    JsonObject jObj = instanceField.getJsonObject(instanceFieldIndex);
    JsonArray subFieldsArray = jObj.getJsonArray("subfield");

    //push into a set so that we can do a lookup for each subfield in the marc instead
    //of looping over the array
    Set<String> subFieldsSet = new HashSet<>(subFieldsArray.getList());

    //it can be a one to one mapping, or there could be rules to apply prior to the mapping
    rules = jObj.getJsonArray("rules");

    //allow to declare a delimiter when concatenating subfields.
    //also allow , in a multi subfield field, to have some subfields with delimiter x and
    //some with delimiter y, and include a separator to separate each set of subfields
    //maintain a delimiter per subfield map - to lookup the correct delimiter and place it in string
    //maintain a string buffer per subfield - but with the string buffer being a reference to the
    //same stringbuilder for subfields with the same delimiter - the stringbuilder references are
    //maintained in the buffers2concat list which is then iterated over and we place a separator
    //between the content of each string buffer reference's content
    delimiters = jObj.getJsonArray("subFieldDelimiter");

    //this is a map of each subfield to the delimiter to delimit it with
    subField2Delimiter.clear();

    //should we run rules on each subfield value independently or on the entire concatenated
    //string, not relevant for non repeatable single subfield declarations or entity declarations
    //with only one non repeatable subfield
    boolean applyPost = false;

    if(jObj.getBoolean("applyRulesOnConcatenatedData") != null){
      applyPost = jObj.getBoolean("applyRulesOnConcatenatedData");
    }

    //map a subfield to a stringbuilder which will hold its content
    //since subfields can be concatenated into the same stringbuilder
    //the map of different subfields can map to the same stringbuilder reference
    subField2Data.clear();

    //keeps a reference to the stringbuilders that contain the data of the
    //subfield sets. this list is then iterated over and used to delimit subfield sets
    buffers2concat.clear();

    handleDelimiters();

    String[] embeddedFields = jObj.getString("target").split("\\.");
    if (!isMappingValid(object, embeddedFields)) {
      LOGGER.debug("bad mapping " + jObj.encode());
      return;
    }
    //iterate over the subfields in the mapping entry
    List<Subfield> subFields = dataField.getSubfields();
    //check if we need to expand the subfields into additional subfields
    JsonObject splitter = jObj.getJsonObject("subFieldSplit");
    if(splitter != null){
      expandSubfields(subFields, splitter);
    }

    for (int subFieldsIndex = 0; subFieldsIndex < subFields.size(); subFieldsIndex++) {
      handleSubFields(subFields, subFieldsIndex, subFieldsSet, arraysOfObjects,
        applyPost,
        embeddedFields);
    }

    if(!(entityRequestedPerRepeatedSubfield && entityRequested)){

      String completeData = generateDataString();
      if(applyPost){
        completeData = processRules(completeData);
      }
      if(createNewObject(embeddedFields, completeData, rememberComplexObj)){
        createNewComplexObj = false;
      }
    }
    ((Instance)object).setId(UUID.randomUUID().toString());
  }

  private void handleSubFields(List<Subfield> subFields, int subFieldsIndex, Set<String> subFieldsSet,
                                  List<Object[]> arraysOfObjects,
                                  boolean applyPost,
                                  String[] embeddedFields) {

    String data = subFields.get(subFieldsIndex).getData();
    char sub1 = subFields.get(subFieldsIndex).getCode();
    String subfield = String.valueOf(sub1);
    if (!subFieldsSet.contains(subfield)) {
      return;
    }

    //rule file contains a rule for this subfield
    if(arraysOfObjects.size() <= subFieldsIndex){
      temporarilySaveObjectsWithMultipleFields(arraysOfObjects, subFieldsIndex);
    }

    if(!applyPost){

      //apply rule on the per subfield data. if applyPost is set to true, we need
      //to wait and run this after all the data associated with this target has been
      //concatenated , therefore this can only be done in the createNewObject function
      //which has the full set of subfield data
      data = processRules(data);
    }

    if (delimiters != null) {
      //delimiters is not null, meaning we have a string buffer for each set of subfields
      //so populate the appropriate string buffer
      if (subField2Data.get(String.valueOf(subfield)).length() > 0) {
        subField2Data.get(String.valueOf(subfield)).append(subField2Delimiter.get(subfield));
      }
      subField2Data.get(subfield).append(data);
    } else {
      StringBuilder sb = buffers2concat.get(0);
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
      createNewComplexObj = arraysOfObjects.get(subFieldsIndex)[0] == null;
      String completeData = generateDataString();
      createNewObject(embeddedFields, completeData, arraysOfObjects.get(subFieldsIndex));
    }
  }

  private void temporarilySaveObjectsWithMultipleFields(List<Object[]> arraysOfObjects, int subFieldsIndex) {
    //temporarily save objects with multiple fields so that the fields of the
    //same object can be populated with data from different subfields
    for (int arraysOfObjectsIndex = arraysOfObjects.size(); arraysOfObjectsIndex <= subFieldsIndex;
         arraysOfObjectsIndex++) {
      arraysOfObjects.add(new Object[] { null });
    }
  }

  private void handleDelimiters() {
    if(delimiters != null){

      for (int j = 0; j < delimiters.size(); j++) {
        JsonObject job = delimiters.getJsonObject(j);
        String delimiter = job.getString(VALUE);
        JsonArray subFieldswithDel = job.getJsonArray("subfields");
        StringBuilder subFieldsStringBuilder = new StringBuilder();
        buffers2concat.add(subFieldsStringBuilder);
        if(subFieldswithDel.size() == 0){
          separator = delimiter;
        }
        for (int k = 0; k < subFieldswithDel.size(); k++) {
          subField2Delimiter.put(subFieldswithDel.getString(k), delimiter);
          subField2Data.put(subFieldswithDel.getString(k), subFieldsStringBuilder);
        }
      }
    }
    else{
      buffers2concat.add(new StringBuilder());
    }
  }

  private String managePushToDB(String tenantId, boolean done,
                                Map<String, String> okapiHeaders) throws JsonProcessingException {

    Object record = object;

    if(importSQLStatement.length() == 0 && record == null && done) {
      //no more marcs to process, we reached the end of the loop, and we have no records in the buffer to flush to the db then just return,
      return null;
    }
    if (importSQLStatement.length() == 0 && !isTest) {
      importSQLStatement
        .append("COPY ")
        .append(tenantId)
        .append("_mod_inventory_storage.instance(_id,jsonb) FROM STDIN  DELIMITER '|' ENCODING 'UTF8';");
      importSQLStatement.append(System.lineSeparator());
    }
    if(record != null){
      importSQLStatement.append(((Instance)record).getId()).append("|").append(ObjectMapperTool.getMapper().writeValueAsString(record)).append(
        System.lineSeparator());
    }
    counter++;
    if (counter == bulkSize || done) {
      counter = 0;
      try {
        if(!isTest){
          importSQLStatement.append("\\.");
          HttpResponse response = post(url + IMPORT_URL , importSQLStatement, okapiHeaders);
          importSQLStatement.delete(0, importSQLStatement.length());
          if (response.getStatusLine().getStatusCode() != 200) {
            String e = IOUtils.toString( response.getEntity().getContent() , "UTF8");
            LOGGER.error(e);
            return e;
          }
        }
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        return e.getMessage();
      }
      return null;
    }
    return null;
  }

  private void processMarcControlSection(Iterator<ControlField> ctrlIter, JsonObject rulesFile)
    throws IllegalAccessException, InstantiationException {
    //iterate over all the control fields in the marc record
    //for each control field , check if there is a rule for mapping that field in the rule file
    while (ctrlIter.hasNext()) {
      ControlField controlField = ctrlIter.next();
      //get entry for this control field in the rules.json file
      JsonArray controlFieldRules = rulesFile.getJsonArray(controlField.getTag());
      if (controlFieldRules != null) {
        handleControlFieldRules(controlFieldRules, controlField);
      }
    }
  }

  private void handleControlFieldRules(JsonArray controlFieldRules, ControlField controlField)
    throws IllegalAccessException, InstantiationException {

    //when populating an object with multiple fields from the same marc field
    //this is used to pass the reference of the previously created object to the buildObject function
    Object[] rememberComplexObj = new Object[]{null};
    createNewComplexObj = true;

    for (int i = 0; i < controlFieldRules.size(); i++) {
      JsonObject cfRule = controlFieldRules.getJsonObject(i);

      //get rules - each rule can contain multiple conditions that need to be met and a
      //value to inject in case all the conditions are met
      rules = cfRule.getJsonArray("rules");

      //the content of the Marc control field
      String data = processRules(controlField.getData());
      if ((data != null) && data.isEmpty()) {
        continue;
      }

      //if conditionsMet = true, then all conditions of a specific rule were met
      //and we can set the target to the rule's value
      String target = cfRule.getString("target");
      String[] embeddedFields = target.split("\\.");

      if (isMappingValid(object, embeddedFields)) {
        Object val = getValue(object, embeddedFields, data);
        LoaderAPI.buildObject(object, embeddedFields, createNewComplexObj, val, rememberComplexObj);
        createNewComplexObj = false;
      } else {
        LOGGER.debug("bad mapping " + rules.encode());
      }
    }
  }

  private String processRules(String data){
    if(rules == null){
      return Escaper.escape(data);
    }

    //there are rules associated with this subfield / control field - to instance field mapping
    String originalData = data;
    for (int ruleIndex = 0; ruleIndex < rules.size(); ruleIndex++) {
      ProcessedSingleItem psi = processRule(rules.getJsonObject(ruleIndex), data, originalData);
      data = psi.getData();
      if (psi.doBreak()) {
        break;
      }
    }
    return Escaper.escape(data);
  }

  private ProcessedSingleItem processRule(JsonObject rule, String data, String originalData) {


    //get the conditions associated with each rule
    JsonArray conditions = rule.getJsonArray("conditions");

    //get the constant value (if is was declared) to set the instance field to in case all
    //conditions are met for a rule, since there can be multiple rules
    //each with multiple conditions, a match of all conditions in a single rule
    //will set the instance's field to the const value. hence, it is an AND
    //between all conditions pr.doBreak() ?and an OR between all rules
    //example of a constant value declaration in a rule:
    //      "rules": [
    //                {
    //                  "conditions": [.....],
    //                  "value": "book"
    //if this value is not indicated, the value mapped to the instance field will be the
    //output of the function - see below for more on that
    String ruleConstVal = rule.getString(VALUE);
    boolean conditionsMet = true;

    //each rule has conditions, if they are all met, then mark
    //continue processing the next condition, if all conditions are met
    //set the target to the value of the rule
    boolean isCustom = false;

    for (int m = 0; m < conditions.size(); m++) {
      JsonObject condition = conditions.getJsonObject(m);

      //1..n functions can be declared within a condition (comma delimited).
      //for example:
      //  A condition with with one function, a parameter that will be passed to the
      //  function, and the expected value for this condition to be met
      //   {
      //        "type": "char_select",
      //        "parameter": "0",
      //        "value": "7"
      //   }
      //the functions here can rely on the single value field for comparison
      //to the output of all functions on the marc's field data
      //or, if a custom function is declared, the value will contain
      //the javascript of the custom function
      //for example:
      //          "type": "custom",
      //          "value": "DATA.replace(',' , ' ');"
      String[] functions = ProcessorHelper.getFunctionsFromCondition(condition);
      isCustom = checkIfAnyFunctionIsCustom(functions, isCustom);

      ProcessedSinglePlusConditionCheck processedCondition =
        processCondition(condition, data, originalData, conditionsMet, ruleConstVal, isCustom);
      data = processedCondition.getData();
      conditionsMet = processedCondition.isConditionsMet();
    }
    if(conditionsMet && ruleConstVal != null && !isCustom){

      //all conditions of the rule were met, and there
      //is a constant value associated with the rule, and this is
      //not a custom rule, then set the data to the const value
      //no need to continue processing other rules for this subfield
      data = ruleConstVal;
      return new ProcessedSingleItem(data, true);
    }
    return new ProcessedSingleItem(data, false);
  }

  private ProcessedSinglePlusConditionCheck processCondition(JsonObject condition, String data, String originalData,
                                                             boolean conditionsMet, String ruleConstVal,
                                                             boolean isCustom) {

    if(leader != null && condition.getBoolean("LDR") != null){

      //the rule also has a condition on the leader field
      //whose value also needs to be passed into any declared function
      data = leader.toString();
    }
    String valueParam = condition.getString(VALUE);
    for (String function : ProcessorHelper.getFunctionsFromCondition(condition)) {
      ProcessedSinglePlusConditionCheck processedFunction =  processFunction(function, data, isCustom, valueParam, condition,
        conditionsMet, ruleConstVal);
      conditionsMet = processedFunction.isConditionsMet();
      data = processedFunction.getData();
      if (processedFunction.doBreak()) {
        break;
      }
    }
    if(!conditionsMet){

      //all conditions for this rule we not met, revert data to the originalData passed in.
      return new ProcessedSinglePlusConditionCheck(originalData, true, false);
    }
    return new ProcessedSinglePlusConditionCheck(data, false, true);
  }

  private ProcessedSinglePlusConditionCheck processFunction(String function, String data, boolean isCustom,
                                                            String valueParam, JsonObject condition,
                                                            boolean conditionsMet, String ruleConstVal) {

    if(CUSTOM.equals(function.trim())){
      try{
        if (valueParam == null) {
          throw new NullPointerException("valueParam == null");
        }
        data = (String)JSManager.runJScript(valueParam, data);
      }
      catch(Exception e){

        //the function has thrown an exception meaning this condition has failed,
        //hence this specific rule has failed
        conditionsMet = false;
        LOGGER.error(e.getMessage(), e);
      }
    }
    else{
      String c = NormalizationFunctions.runFunction(function.trim(), data, condition.getString("parameter"));
      if(valueParam != null && !c.equals(valueParam) && !isCustom){

        //still allow a condition to compare the output of a function on the data to a constant value
        //unless this is a custom javascript function in which case, the value holds the custom function
        return new ProcessedSinglePlusConditionCheck(data, true, false);
      }
      else if (ruleConstVal == null){

        //if there is no val to use as a replacement , then assume the function
        //is doing generating the needed value and set the data to the returned value
        data = c;
      }
    }
    return new ProcessedSinglePlusConditionCheck(data, false, conditionsMet);
  }

  private boolean checkIfAnyFunctionIsCustom(String[] functions, boolean isCustom) {

    //we need to know if one of the functions is a custom function
    //so that we know how to handle the value field - the custom indication
    //may not be the first function listed in the function list
    //a little wasteful, but this will probably only loop at most over 2 or 3 function names
    for (String function : functions) {
      if(CUSTOM.equals(function.trim())){
        isCustom = true;
        break;
      }
    }
    return isCustom;
  }

  /**
   * create the need part of the instance object based on the target and the string containing the
   * content per subfield sets
   * @param embeddedFields - the targer
   * @param rememberComplexObj - the current object within the instance object we are currently populating
   * this can be null if we are now creating a new object within the instance object
   * @return
   */
  private boolean createNewObject(String[] embeddedFields, String data, Object[] rememberComplexObj) {

    if(data.length() != 0){
      Object val = getValue(object, embeddedFields, data);
      try {
        return LoaderAPI.buildObject(object, embeddedFields, createNewComplexObj, val, rememberComplexObj);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        return false;
      }
    }
    return false;
  }

  /**
   * buffers2concat - list of string buffers, each one representing the data belonging to a set of
   * subfields concatenated together, so for example, 2 sets of subfields will mean two entries in the list
   * @return
   */
  private String generateDataString(){
    StringBuilder finalData = new StringBuilder();
    for (StringBuilder sb : buffers2concat) {
      if(sb.length() > 0){
        if(finalData.length() > 0){
          finalData.append(separator);
        }
        finalData.append(sb);
      }
    }
    return finalData.toString();
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
    String func = splitConf.getString(TYPE);
    boolean isCustom = false;
    if(CUSTOM.equals(func)){
      isCustom = true;
    }
    String param = splitConf.getString(VALUE);
    for (Subfield sub : subs) {
      String data = sub.getData();
      Iterator<?> splitData;
      if(isCustom){
        try {
          splitData = ((jdk.nashorn.api.scripting.ScriptObjectMirror)JSManager.runJScript(param, data)).values().iterator();
        } catch (Exception e) {
          LOGGER.error("Expanding a field via subFieldSplit must return an array of results. ");
          throw e;
        }
      }
      else{
        splitData = NormalizationFunctions.runSplitFunction(func, data, param);
      }
      while (splitData.hasNext()) {
        String newData = (String)splitData.next();
        Subfield expandedSub = new SubfieldImpl(sub.getCode(), newData);
        expandedSubs.add(expandedSub);
      }
    }
    subs.clear();
    subs.addAll(expandedSubs);
  }

  private HttpResponse post(String url, StringBuilder data, Map<String, String> okapiHeaders) throws IOException {
    RequestConfig config = RequestConfig.custom()
      .setConnectTimeout(CONNECT_TIMEOUT)
      .setConnectionRequestTimeout(CONNECTION_TIMEOUT)
      .setSocketTimeout(SO_TIMEOUT)
      .build();
    try (CloseableHttpClient httpclient = HttpClientBuilder.create().setDefaultRequestConfig(config).build()) {
      HttpPost httpPost = new HttpPost(url);
      StringEntity stringEntity = new StringEntity(data.toString(), "UTF8");
      httpPost.setEntity(stringEntity);
      httpPost.setHeader(RestVerticle.OKAPI_HEADER_TENANT,
        okapiHeaders.get(RestVerticle.OKAPI_HEADER_TENANT));
      httpPost.setHeader(RestVerticle.OKAPI_HEADER_TOKEN,
        okapiHeaders.get(RestVerticle.OKAPI_HEADER_TOKEN));
      httpPost.setHeader(RestVerticle.OKAPI_USERID_HEADER,
        okapiHeaders.get(RestVerticle.OKAPI_USERID_HEADER));
      httpPost.setHeader("Content-type", "application/octet-stream");
      httpPost.setHeader("Accept", "text/plain");
      return httpclient.execute(httpPost);
    }
  }

  void processStatic(String url, boolean isTest, InputStream entity, Handler<AsyncResult<Response>> asyncResultHandler,
                     Context vertxContext){
    this.isTest = isTest;
    this.url = url;
    vertxContext.owner().executeBlocking( block -> {
      try {
        processStaticBlock(block, entity);
      } catch (Exception e) {
        LOGGER.error(e.getMessage(), e);
        block.fail(e);
      }
    }, true, whenDone -> {
      if(whenDone.succeeded()){
        if(!isTest){
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            LoadResource.PostLoadStaticResponse.withCreated(whenDone.result().toString())));
        }else{
          asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
            LoadResource.PostLoadStaticTestResponse.withPlainCreated(whenDone.result().toString())));
        }
      }
      else{
        asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(
          LoadResource.PostLoadStaticResponse.withPlainBadRequest(whenDone.cause().getMessage())));
      }
      LOGGER.info("Completed processing of REQUEST");
    });
  }

  private void processStaticBlock(Future<Object> block, InputStream entity)
    throws IOException {

    LOGGER.info("REQUEST ID " + UUID.randomUUID().toString());
    tenantId = TenantTool.calculateTenantId(okapiHeaders.get(ClientGenerator.OKAPI_HEADER_TENANT));
    String content = IoUtil.toStringUtf8(entity);
    JsonObject jobj = new JsonObject(content);
    String error = validateStaticLoad(jobj);

    if(error != null){
      block.fail(error);
      return;
    }

    StringBuilder importSQLStatementMethod = new StringBuilder();
    boolean isArray = JsonValidator.isValidJsonArray(jobj.getValue(VALUES).toString());
    List<JsonObject> listOfRecords;

    if(isArray){
      listOfRecords = contentArray2list(jobj);
    } else{
      listOfRecords = contentObject2list(jobj);
    }

    if(listOfRecords.isEmpty()){
      block.fail("No records to process...");
      return;
    }

    appendStringsToSQLStatementIfNotTest(importSQLStatementMethod, jobj);

    for (JsonObject record : listOfRecords) {
      String id = insertRandomUUID(record);
      String persistRecord = record.encode().replaceAll("\\$\\{randomUUID\\}", id);
      importSQLStatementMethod.append(id).append("|").append(persistRecord).append(System.lineSeparator());
    }

    if(!isTest){
      importSQLStatementMethod.append("\\.");
      HttpResponse response = post(url + IMPORT_URL , importSQLStatementMethod, okapiHeaders);
      if (response.getStatusLine().getStatusCode() != 200) {
        String e = IOUtils.toString( response.getEntity().getContent() , "UTF8");
        LOGGER.error(e);
        block.fail(e);
      }
    }
    block.complete(importSQLStatementMethod);
  }

  private void appendStringsToSQLStatementIfNotTest(StringBuilder importSQLStatementMethod, JsonObject jobj) {
    if(!isTest){
      importSQLStatementMethod
        .append("COPY ")
        .append(tenantId)
        .append("_mod_inventory_storage.").append(jobj.getString(TYPE))
        .append("(_id,jsonb) FROM STDIN  DELIMITER '|' ENCODING 'UTF8';")
        .append(System.lineSeparator());
    }
  }

  private String insertRandomUUID(JsonObject record) {
    String id = record.getString("id");
    if(id == null || "${randomUUID}".equals(id)){
      id = UUID.randomUUID().toString();
    }
    return id;
  }

  private String validateStaticLoad(JsonObject jobj){
    String table = jobj.getString(TYPE);
    JsonObject record = jobj.getJsonObject(RECORD);
    Object values = jobj.getValue(VALUES);

    if((table == null && !isTest)){
      return "type field (table name) must be defined in input";
    }
    else if(record == null){
      return "record field must be defined in input";
    }
    else if(values == null){
      return "values field must be defined in input";
    }
    return null;
  }

  private List<JsonObject> contentArray2list(JsonObject jobj){
    JsonArray values = jobj.getJsonArray(VALUES);
    JsonObject record = jobj.getJsonObject(RECORD);
    List<JsonObject> listOfRecords = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) {
      JsonObject template = record.copy();
      JsonObject toinject = values.getJsonObject(i);
      template.mergeIn(toinject, true);
      listOfRecords.add(template);
    }
    return listOfRecords;
  }

  private List<JsonObject> contentObject2list(JsonObject jobj){
    JsonObject values = jobj.getJsonObject(VALUES);
    JsonObject record = jobj.getJsonObject(RECORD);
    List<JsonObject> listOfRecords = new ArrayList<>();
    values.forEach( entry -> {
      String field = entry.getKey();
      JsonArray vals =  (JsonArray)entry.getValue();
      for (int i = 0; i < vals.size(); i++) {
        JsonObject j = record.copy();
        Object o = vals.getValue(i);
        j.put(field, o);
        listOfRecords.add(j);
      }
    });
    return listOfRecords;
  }

  private static Object getValue(Object object, String[] path, String value) {
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
}
