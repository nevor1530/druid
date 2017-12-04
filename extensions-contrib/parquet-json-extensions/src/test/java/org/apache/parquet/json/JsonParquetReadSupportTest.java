package org.apache.parquet.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class JsonParquetReadSupportTest {

    @Test
    public void extractPureName() {
        Pattern pattern = JsonParquetReadSupport.JSON_PATH_REGEX;
        JsonParquetReadSupport support = new JsonParquetReadSupport();

        String expr = "$.a.b.c";
        assertEquals("a.b.c", support.extractPureName(expr));

        expr = "$.a.b[2].c";
        assertEquals("a.b.c", support.extractPureName(expr));

        expr = "a.b.c";
        try {
            support.extractPureName(expr);
            assertEquals(1, 2);
        } catch (Exception e) {
        }
    }

    @Test
    public void parseToTree() throws JsonProcessingException {
        JsonParquetReadSupport support = new JsonParquetReadSupport();
        Set<String> paths = new HashSet<>();
        paths.add("a.b.c");
        paths.add("a.b.d");
        paths.add("e.f.g");
        paths.add("e.b.c");

        Map map = support.paseToTree(paths);

        assertEquals(true, map.containsKey("a"));
        assertEquals(true, map.containsKey("e"));
        assertEquals(false, map.containsKey("b"));
        assertEquals(true, ((Map)map.get("a")).containsKey("b"));
        assertEquals(true, ((Map)((Map)map.get("a")).get("b")).containsKey("d"));
    }
}