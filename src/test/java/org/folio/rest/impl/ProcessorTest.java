package org.folio.rest.impl;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class ProcessorTest {

  private Processor processor;

  @Before
  public void setUp() throws Exception {

    Map<String, String> okapiHeaders = new HashMap<>();
    processor = new Processor("testTenantId", okapiHeaders);

  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void sourceRecordHandlingTest() {

//    processor.process(false, );

  }
}
