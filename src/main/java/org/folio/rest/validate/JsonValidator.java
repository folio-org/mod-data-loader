package org.folio.rest.validate;

import java.io.IOException;
import java.io.InputStream;

import org.folio.rest.tools.utils.ObjectMapperTool;

import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * @author shale
 *
 */
public class JsonValidator {

  public static boolean isValidJsonArray(InputStream entity) throws JsonProcessingException, IOException{
    return ObjectMapperTool.getMapper().readTree(entity).isArray();
  }

  public static boolean isValidJsonArray(String entity) throws JsonProcessingException, IOException{
    return ObjectMapperTool.getMapper().readTree(entity).isArray();
  }

  public static boolean isValidJsonObject(InputStream entity) throws JsonProcessingException, IOException{
    return ObjectMapperTool.getMapper().readTree(entity).isObject();
  }

}
