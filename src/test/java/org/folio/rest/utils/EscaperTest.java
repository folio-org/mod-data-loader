package org.folio.rest.utils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

public class EscaperTest {

  private static final Logger LOGGER = LogManager.getLogger(EscaperTest.class);

  @Test
  public void escapeSqlCopyFromTest() {
    LOGGER.info("\n---\nescapeSqlCopyFromTest()\n---");

    String backslashWithDoubleQuote ="\\\"";
    String escapedBackslashWithDoubleQuote = "\\\\\"";

    LOGGER.info("[" + backslashWithDoubleQuote + "] ---> [" + escapedBackslashWithDoubleQuote + "]");
    LOGGER.info("[|] ---> [\\|]");
    LOGGER.info("[\\n] ---> [\\\\n]");
    LOGGER.info("[\\r] ---> [\\\\r]");

    assertEquals("foo\\\\bar", Escaper.escapeSqlCopyFrom("foo\\bar"));
    assertEquals("foo\\\\bar\\\\foo", Escaper.escapeSqlCopyFrom("foo\\bar\\foo"));
    assertEquals("newline \\\\n carriage return \\\\r",
      Escaper.escapeSqlCopyFrom("newline \\n carriage return \\r"));
    assertEquals("\\|", Escaper.escapeSqlCopyFrom("|"));
    assertEquals("\\\n", Escaper.escapeSqlCopyFrom("\n"));
    assertEquals("\\\r", Escaper.escapeSqlCopyFrom("\r"));
    assertEquals(escapedBackslashWithDoubleQuote, Escaper.escapeSqlCopyFrom(backslashWithDoubleQuote));
  }
}
