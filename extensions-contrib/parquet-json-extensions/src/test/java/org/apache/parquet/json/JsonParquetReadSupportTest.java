/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

public class JsonParquetReadSupportTest
{

  @Test
  public void extractPureName()
  {
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
    }
    catch (Exception e) {
    }
  }

  @Test
  public void parseToTree() throws JsonProcessingException
  {
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
    assertEquals(true, ((Map) map.get("a")).containsKey("b"));
    assertEquals(true, ((Map) ((Map) map.get("a")).get("b")).containsKey("d"));
  }

  @Test
  public void fetchRequriedPaths()
  {
    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromFile(new File("example/newer_schema_config.json"));
    JsonParquetReadSupport support = new JsonParquetReadSupport();
    Set set = support.fetchRequriedPaths(config);
    assertEquals(196, set.size());
  }
}