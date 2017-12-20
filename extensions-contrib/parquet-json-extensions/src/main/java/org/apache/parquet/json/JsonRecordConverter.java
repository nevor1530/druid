package org.apache.parquet.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

public class JsonRecordConverter extends GroupConverter {

    private final Converter[] converters;

    private final ObjectMapper om;
    private GroupType parquetSchema;
    private ParentValueContainer pvc;
    private ObjectNode currentObj = null;
    private int fieldIndex = 0;
    private String fieldName = null;

    public JsonRecordConverter(ParentValueContainer pvc, GroupType parquetSchema) {
        this.pvc = pvc;
        this.parquetSchema = parquetSchema;

        om = new ObjectMapper();
        int count = parquetSchema.getFieldCount();
        converters = new Converter[count];
        for (int i = 0; i < count; i++) {
            Type type = parquetSchema.getType(i);
            converters[i] = newConverter(type);
        }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
        return converters[fieldIndex];
    }

    public ObjectNode getValueContainer() {
        return currentObj;
    }

    public Converter newConverter(final Type type) {
        final String childName = type.getName();
        ParentValueContainer parentValueContainer = null;
        if (type.isRepetition(Type.Repetition.REPEATED)) {
            parentValueContainer = new ParentValueContainer() {
                @Override
                public void add(Object value) {
                    ObjectNode parentNode = getValueContainer();
                    if (!parentNode.has(childName)) {
                        parentNode.set(childName, om.createArrayNode());
                    }
                    JsonUtils.jsonNodeSetOrAdd(parentNode.get(childName), value, null);
                }
            };
        } else {
            parentValueContainer = new ParentValueContainer() {
                @Override
                public void add(Object value) {
                    JsonUtils.jsonNodeSetOrAdd(getValueContainer(), value, childName);
                }
            };
        }

        if (type.isPrimitive()) {
            return new JsonPrimitiveConvrter(parentValueContainer, type.asPrimitiveType());
        } else {
            return new JsonRecordConverter(parentValueContainer, type.asGroupType());
        }
    }

    @Override
    public void start() {
        currentObj = om.createObjectNode();
    }

    @Override
    public void end() {
        if (pvc != null) {
            if (currentObj.size() == 0) {
                pvc.add(null);
            } else {
                pvc.add(currentObj);
            }
        }
    }

    public ObjectNode getCurrentRecord() {
        return currentObj;
    }

    public static class JsonPrimitiveConvrter extends PrimitiveConverter {
        private final ParentValueContainer pvc;

        public JsonPrimitiveConvrter(ParentValueContainer pvc, PrimitiveType type) {
            this.pvc = pvc;
        }

        @Override
        public void addBinary(Binary value) {
            pvc.add(value.toStringUsingUTF8());
        }

        @Override
        public void addBoolean(boolean value) {
            pvc.add(new Boolean(value));
        }

        @Override
        public void addDouble(double value) {
            pvc.add(new Double(value));
        }

        @Override
        public void addFloat(float value) {
            pvc.add(new Float(value));
        }

        @Override
        public void addInt(int value) {
            pvc.add(new Integer(value));
        }

        @Override
        public void addLong(long value) {
            pvc.add(new Long(value));
        }
    }
}
