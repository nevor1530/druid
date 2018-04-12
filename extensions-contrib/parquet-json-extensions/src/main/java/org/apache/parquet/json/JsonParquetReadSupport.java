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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.java.util.common.parsers.JSONPathFieldSpec;
import io.druid.java.util.common.parsers.JSONPathFieldType;
import io.druid.java.util.common.parsers.JSONPathSpec;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class JsonParquetReadSupport extends ReadSupport<ObjectNode>
{
  public static final Pattern JSON_PATH_REGEX = Pattern.compile("^\\$(\\.\\w+(?:\\[.*?\\])?)+$");

  /**
   * indicate whether auto fetch all primitive fields， boolean type with default false
   */
  public static final String FETCH_PRIMITIVE_FIELDS = "parquet.json.fetch_primitive_fields";

  /**
   * 从 aggregators, timestamp column, dimension schema 里取出「所需字段」集合，
   * 然后从这个集合里去掉 JsonParseSpec.flatternSpec.fields.name 里包含的
   * 再从 JsonParseSpec.flatternSpec.fields 里找出 type=path 的 expr 集合，
   * 用正则解析出 a.b.c 样式的字段集合，合并到「所需字段」集合里
   *
   * @param config
   *
   * @return
   */
  public Set<String> fetchRequriedPaths(HadoopDruidIndexerConfig config)
  {
    Set<String> requiredPaths = new HashSet<>();
    // timestamp column
    String tsField = config.getParser().getParseSpec().getTimestampSpec().getTimestampColumn();
    requiredPaths.add(tsField);

    // dimensions
    List<DimensionSchema> dimensionSchema = config.getParser().getParseSpec().getDimensionsSpec().getDimensions();
    for (DimensionSchema dim : dimensionSchema) {
      requiredPaths.add(dim.getName());
    }

    // metrics
    Set<String> metricsFields = Sets.newHashSet();
    for (AggregatorFactory agg : config.getSchema().getDataSchema().getAggregators()) {
      requiredPaths.addAll(agg.requiredFields());
    }

    ParseSpec parseSpec = config.getParser().getParseSpec();
    if (!(parseSpec instanceof JSONParseSpec)) {
      throw new RuntimeException("JsonParquetInputFormat needs json parse spec");
    }
    JSONPathSpec jsonPathSpec = ((JSONParseSpec) parseSpec).getFlattenSpec();
    for (JSONPathFieldSpec field : jsonPathSpec.getFields()) {
      if (field.getType() == JSONPathFieldType.PATH) {
        // 从集合中去掉别名
        requiredPaths.remove(field.getName());
        String pureName = extractPureName(field.getExpr());
        requiredPaths.add(pureName);
      } else if (field.getType() == JSONPathFieldType.ROOT) {
        requiredPaths.add(field.getName());
      }
    }
    return requiredPaths;
  }

  /**
   * 从 FlatternSpec 中的 JsonPath 中解析出纯净的字段名，去掉中括号里的东西。
   * 只支持  $.a.b[*].c 的格式
   *
   * @param expr
   *
   * @return
   */
  public String extractPureName(String expr)
  {
    if (!JSON_PATH_REGEX.matcher(expr).find()) {
      throw new RuntimeException("Unsupport expr: " + expr);
    }
    expr = expr.replaceAll("\\[.*?\\]", "");
    return expr.substring(2);
  }

  private MessageType getPartialReadSchema(InitContext context)
  {
    boolean fetchPrimitive = context.getConfiguration().getBoolean(FETCH_PRIMITIVE_FIELDS, false);

    MessageType fullSchema = context.getFileSchema();

    String name = fullSchema.getName();

    // 如果没有指定 config，则读取全量的 Schema 和 数据
    if (context.getConfiguration().get("druid.indexer.config") == null) {
      return fullSchema;
    }

    HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    Set<String> requiredPaths = fetchRequriedPaths(config);
    // 解析成树状
    Map<String, Object> fieldTree = paseToTree(requiredPaths);

    // TODO 如果设置了 fetch_primitive_field 需要验证 json 字段别名不会跟其它 primitive 字段名字冲突
    // 同时把剩余的 primitive 字段加入
    if (fetchPrimitive) {
      addPrimitiveFields(fullSchema, fieldTree, config);
    }

    List<Type> partialFields = fetchRequriedTypes(fullSchema, fieldTree);
    if (partialFields.size() == 0) {
      try {
        String fieldTreeJson = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(fieldTree);
        throw new RuntimeException("Partial schema is empty:\n" + fieldTreeJson);
      }
      catch (JsonProcessingException e) {
        throw new RuntimeException("Partial schema is empty.");
      }
    }

    return new MessageType(name, partialFields);
  }

  /**
   * @param fullSchema
   * @param fieldTree
   * @param config
   */
  private void addPrimitiveFields(
      MessageType fullSchema,
      Map<String, Object> fieldTree,
      HadoopDruidIndexerConfig config
  )
  {
    Set<String> aliasSet = new HashSet<>();
    JSONPathSpec jsonPathSpec = ((JSONParseSpec) config.getParser().getParseSpec()).getFlattenSpec();
    for (JSONPathFieldSpec field : jsonPathSpec.getFields()) {
      aliasSet.add(field.getName());
    }

    for (Type type : fullSchema.getFields()) {
      if (type.isPrimitive()) {
        // 判断是否有别名冲突
        if (aliasSet.contains(type.getName()) && !fieldTree.containsKey(type.getName())) {
          throw new RuntimeException(String.format(
              "Column '%s' is conflict with other primitive field",
              type.getName()
          ));
        } else {
          fieldTree.put(type.getName(), null);
        }
      }
    }
  }

  private List<Type> fetchRequriedTypes(GroupType schema, Map<String, Object> fieldTree)
  {
    List<Type> types = new LinkedList<>();
    for (Map.Entry<String, Object> entry : fieldTree.entrySet()) {
      // 新 schema 读老 schema 时会遇到未知的字段，忽略之
      if (!schema.containsField(entry.getKey())) {
        continue;
      }
      Type subType = schema.getType(entry.getKey());
      if (entry.getValue() == null) {
        types.add(subType);
      } else {
        List<Type> subTypes = fetchRequriedTypes(subType.asGroupType(), (Map) entry.getValue());
        if (subTypes.size() > 0) {
          types.add(subType.asGroupType().withNewFields(subTypes));
        }
      }
    }
    return types;
  }

  /**
   * 把路径解析成树状
   *
   * @param requiredPaths
   *
   * @return
   */
  public Map<String, Object> paseToTree(Set<String> requiredPaths)
  {
    Map<String, Object> node = new LinkedHashMap<>();
    Map<String, Object> current = node;
    for (String path : requiredPaths) {
      String[] paths = path.split("\\.");
      current = node;
      for (int i = 0; i < paths.length; i++) {
        if (i == paths.length - 1) {
          // 叶子节点，值存 null
          current.put(paths[i], null);
        } else {
          if (!current.containsKey(paths[i])) {
            current.put(paths[i], new LinkedHashMap<String, String>());
          }
          current = (Map<String, Object>) current.get(paths[i]);
        }
      }
    }
    return node;
  }

  @Override
  public ReadContext init(InitContext context)
  {
    MessageType requestedProjection = getSchemaForRead(context.getFileSchema(), getPartialReadSchema(context));
    return new ReadContext(requestedProjection);
  }

  @Override
  public RecordMaterializer<ObjectNode> prepareForRead(
      Configuration configuration,
      Map<String, String> keyValueMetaData,
      MessageType fileSchema,
      ReadContext readContext
  )
  {
    return new JsonRecordMaterializer(readContext.getRequestedSchema());
  }
}
