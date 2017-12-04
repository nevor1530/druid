package org.apache.parquet.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import io.druid.data.input.impl.*;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.query.aggregation.AggregatorFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.*;
import java.util.regex.Pattern;

public class JsonParquetReadSupport extends ReadSupport<ObjectNode> {
    public static final Pattern JSON_PATH_REGEX = Pattern.compile("^\\$(\\.\\w+(?:\\[.*?\\])?)+$");

    /**
     * 从 aggregators, timestamp column, dimension schema 里取出「所需字段」集合，
     * 然后从这个集合里去掉 JsonParseSpec.flatternSpec.fields.name 里包含的
     * 再从 JsonParseSpec.flatternSpec.fields 里找出 type=path 的 expr 集合，
     * 用正则解析出 a.b.c 样式的字段集合，合并到「所需字段」集合里
     * @param config
     * @return
     */
    private Set<String> fetchRequriedPaths(HadoopDruidIndexerConfig config) {
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
        for (JSONPathFieldSpec field: jsonPathSpec.getFields()) {
            if (field.getType() != JSONPathFieldType.PATH) {
                continue;
            }
            // 从集合中去掉别名
            requiredPaths.remove(field.getName());
            String pureName = extractPureName(field.getExpr());
            requiredPaths.add(pureName);
        }
        return requiredPaths;
    }

    /**
     * 从 FlatternSpec 中的 JsonPath 中解析出纯净的字段名，去掉中括号里的东西。
     * 只支持  $.a.b[*].c 的格式
     * @param expr
     * @return
     */
    public String extractPureName(String expr) {
        if (!JSON_PATH_REGEX.matcher(expr).find()) {
            throw new RuntimeException("Unsupport expr: " + expr);
        }
        expr = expr.replaceAll("\\[.*?\\]", "");
        return expr.substring(2);
    }

    private MessageType getPartialReadSchema(InitContext context)
    {
        MessageType fullSchema = context.getFileSchema();

        String name = fullSchema.getName();

        HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
        Set<String> requiredPaths = fetchRequriedPaths(config);
        // TODO 解析成树状
        Map<String, Object> fieldTree = paseToTree(requiredPaths);

        List<Type> partialFields = fetchRequriedTypes(fullSchema, fieldTree);

        return new MessageType(name, partialFields);
    }

    private List<Type> fetchRequriedTypes(GroupType schema, Map<String, Object> fieldTree) {
        List<Type> types = new LinkedList<>();
        for(Map.Entry<String, Object> entry: fieldTree.entrySet()) {
            Type subType = schema.getType(entry.getKey());
            if (entry.getValue() == null) {
                types.add(subType);
            } else {
                List<Type> subTypes = fetchRequriedTypes(subType.asGroupType(), (Map)entry.getValue());
                types.add(subType.asGroupType().withNewFields(subTypes));
            }
        }
        return types;
    }

    /**
     * 把路径解析成树状
     * @param requiredPaths
     * @return
     */
    public Map<String,Object> paseToTree(Set<String> requiredPaths) {
        Map<String, Object> node = new LinkedHashMap<>();
        Map<String, Object> current = node;
        for (String path: requiredPaths) {
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
    public RecordMaterializer<ObjectNode> prepareForRead(Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema, ReadContext readContext) {
        return new JsonRecordMaterializer(readContext.getRequestedSchema());
    }
}
