package org.folio.rest.validate;

import java.io.IOException;

import org.folio.rest.tools.utils.ObjectMapperTool;

import com.fasterxml.jackson.core.JsonParseException;

/**
 * Utility class for validating JSON.
 */
public class JsonValidator {
  private JsonValidator() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  /**
   * @param entity  String to parse as JSON
   * @return True if entity is a String representation of a JSON array, false otherwise.
   * @throws JsonParseException  if entity cannot be parsed as JSON
   * @throws IOException  on missing input
   */
  public static boolean isValidJsonArray(String entity) throws IOException {
    return ObjectMapperTool.getMapper().readTree(entity).isArray();
  }
}
