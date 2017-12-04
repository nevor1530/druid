package org.apache.parquet.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class JsonUtils {
    public static void jsonNodeSetOrAdd(JsonNode node, Object value, String field) {
        if (node.isArray()) {
            arrayNodeAdd((ArrayNode) node, value);
        } else {
            if (field == null) {
                throw new RuntimeException("field should not be null here");
            }
            objectNodeSet((ObjectNode) node, field, value);
        }
    }

    public static void objectNodeSet(ObjectNode node, String field, Object value) {
        if (value == null) {
            node.set(field, null);
            return;
        }
        if (value instanceof String) {
            node.put(field, (String) value);
        } else if (value instanceof Boolean) {
            node.put(field, (Boolean) value);
        } else if (value instanceof Double) {
            node.put(field, (Double) value);
        } else if (value instanceof Float) {
            node.put(field, (Float) value);
        } else if (value instanceof Integer) {
            node.put(field, (Integer) value);
        } else if (value instanceof Long) {
            node.put(field, (Long) value);
        } else if (value instanceof JsonNode) {
            node.set(field, (JsonNode) value);
        } else {
            throw new RuntimeException("should not happen with the type: " + value.getClass());
        }
    }

    public static void arrayNodeAdd(ArrayNode node, Object value) {
        if (value == null) {
            node.add((JsonNode) null);
            return;
        }
        if (value instanceof String) {
            node.add((String) value);
        } else if (value instanceof Boolean) {
            node.add((Boolean) value);
        } else if (value instanceof Double) {
            node.add((Double) value);
        } else if (value instanceof Float) {
            node.add((Float) value);
        } else if (value instanceof Integer) {
            node.add((Integer) value);
        } else if (value instanceof Long) {
            node.add((Long) value);
        } else if (value instanceof JsonNode) {
            node.add((JsonNode) value);
        } else {
            throw new RuntimeException("should not happen with the type: " + value.getClass());
        }
    }
}
