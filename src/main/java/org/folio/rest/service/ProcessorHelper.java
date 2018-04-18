package org.folio.rest.service;

import io.vertx.core.json.JsonObject;

public class ProcessorHelper {

  private ProcessorHelper() {}

  public static String[] getFunctionsFromCondition(JsonObject condition) {
    return condition.getString("type").split(",");
  }
}
