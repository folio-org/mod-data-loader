package org.folio.rest.model;

import io.vertx.core.json.JsonObject;

public class SourceRecord {

  private String id;
  private JsonObject sourceJson;

  public SourceRecord(String id, JsonObject sourceJson) {
    this.id = id;
    this.sourceJson = sourceJson;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public JsonObject getSourceJson() {
    return sourceJson;
  }

  public void setSourceJson(JsonObject sourceJson) {
    this.sourceJson = sourceJson;
  }
}
