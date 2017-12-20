package org.apache.parquet.json;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

public class JsonRecordMaterializer extends RecordMaterializer<ObjectNode> {
    private final JsonRecordConverter root;

    public JsonRecordMaterializer(MessageType parquetSchema) {
        root = new JsonRecordConverter(null, parquetSchema);
    }

    @Override
    public ObjectNode getCurrentRecord() {
        return root.getCurrentRecord();
    }

    @Override
    public GroupConverter getRootConverter() {
        return root;
    }
}
