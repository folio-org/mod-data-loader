package org.folio.rest.impl;

import java.util.Iterator;

import com.google.common.base.Splitter;

/**
 * @author shale
 *
 */
public class NormalizationFunctions {

  public static final String CHAR_SELECT = "char_select";
  public static final String REMOVE_ENDING_PUNC = "remove_ending_punc";
  public static final String TRIM = "trim";
  public static final String TRIM_PERIOD = "trim_period";
  public static final String SPLIT_FUNCTION_SPLIT_EVERY = "split_every";

  public static Iterator<?> runSplitFunction(String funcName, String val, String param){
    if(val == null){
      return null;
    }
    if(SPLIT_FUNCTION_SPLIT_EVERY.equals(funcName)){
      return splitEvery(val, param);
    }
    return null;
  }

  public static String runFunction(String funcName, String val, String param){
    if(val == null){
      return "";
    }
    if(CHAR_SELECT.equals(funcName)){
      return charSelect(val, param);
    }
    else if(REMOVE_ENDING_PUNC.equals(funcName)){
      return removeEndingPunc(val);
    }
    else if(TRIM.equals(funcName)){
      return trim(val);
    }
    else if(TRIM_PERIOD.equals(funcName)){
      return trimPeriod(val);
    }
    return "";
  }

  private static Iterator<String> splitEvery(String val, String param) {
    return Splitter.fixedLength(Integer.parseInt(param)).split(val).iterator();
  }

  public static String charSelect(String val, String pos){
    try{
      if(pos.contains("-")){
        String []range = pos.split("-");
        return val.substring(Integer.parseInt(range[0])-1, Integer.parseInt(range[1]));
      }
      int p = Integer.parseInt(pos);
      return val.substring(p,p+1);
    }
    catch(Exception e){
      return "";
    }
  }

  public static String trimPeriod(final String input){
    if('.' == input.charAt(input.length()-1)){
      return input.substring(0, input.length()-1);
    }
    return input;
  }

  public static String trim(String val){
    return val.trim();
  }

  public static String removeEndingPunc(String val){
    return modified(val, (val.length()-1));
  }

  private static String modified(final String input, int pos){
    if(!Character.isAlphabetic(input.charAt(pos)) && !Character.isDigit(input.charAt(pos))){
      return input.substring(0, input.length()-1);
    }
    return input;
  }
}
