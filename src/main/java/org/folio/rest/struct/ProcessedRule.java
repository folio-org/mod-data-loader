package org.folio.rest.struct;

public class ProcessedRule {

  private String data;
  private boolean shouldBreak;

  public ProcessedRule(String data, boolean doBreak) {
    this.data = data;
    this.shouldBreak = doBreak;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }

  public boolean doBreak() {
    return shouldBreak;
  }

  public void setShouldBreak(boolean shouldBreak) {
    this.shouldBreak = shouldBreak;
  }
}
