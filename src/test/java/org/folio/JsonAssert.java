package org.folio;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class JsonAssert {

    static ObjectMapper mapper = new ObjectMapper();

    static void areEqual(String json1, String json2) throws JsonProcessingException, IOException {
        JsonNode tree1 = mapper.readTree(json1);
        JsonNode tree2 = mapper.readTree(json2);

        if(!tree1.equals(tree2)){
          System.out.println("\n"+tree1);
          System.out.println("--------------");
          System.out.println(tree2);
        }

        assert tree1.equals(tree2);
    }

}
