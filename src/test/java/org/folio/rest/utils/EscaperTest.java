package org.folio.rest.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

public class EscaperTest {

  private static final Logger LOGGER = LogManager.getLogger(EscaperTest.class);

  @Test
  public void backslashEscapeTest() {
    LOGGER.info("\n---\nbackslashEscapeTest()\n---");

    String backslashWithDoubleQuote ="\\\"";
    String escapedBackslashWithDoubleQuote = "\\\\\"";

    LOGGER.info("[" + backslashWithDoubleQuote + "] ---> [" + escapedBackslashWithDoubleQuote + "]");

    assertEquals("foo\\\\bar", Escaper.backslashEscape("foo\\bar"));
    assertEquals("foo\\\\bar\\\\foo", Escaper.backslashEscape("foo\\bar\\foo"));
    assertEquals(escapedBackslashWithDoubleQuote, Escaper.backslashEscape(backslashWithDoubleQuote));
  }
}
