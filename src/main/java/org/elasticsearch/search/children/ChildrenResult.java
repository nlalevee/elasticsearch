package org.elasticsearch.search.children;

import static org.elasticsearch.search.highlight.HighlightField.readHighlightField;

import java.io.IOException;
import java.util.Map;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.highlight.HighlightField;

import com.google.common.collect.ImmutableMap;

public class ChildrenResult implements Streamable {

    private Map<String, HighlightField> highlightFields;

    private String type;

    private String id;

    public ChildrenResult() {
        // default constructor
    }

    public ChildrenResult(String type, String id, Map<String, HighlightField> highlightFields) {
        this.highlightFields = highlightFields;
    }

    public String type() {
        return type;
    }

    public String id() {
        return id;
    }

    public Map<String, HighlightField> highlightFields() {
        return highlightFields;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        id = in.readUTF();
        type = in.readUTF();
        int size = in.readVInt();
        if (size == 0) {
            highlightFields = ImmutableMap.of();
        } else if (size == 1) {
            HighlightField field = readHighlightField(in);
            highlightFields = ImmutableMap.of(field.name(), field);
        } else if (size == 2) {
            HighlightField field1 = readHighlightField(in);
            HighlightField field2 = readHighlightField(in);
            highlightFields = ImmutableMap.of(field1.name(), field1, field2.name(), field2);
        } else if (size == 3) {
            HighlightField field1 = readHighlightField(in);
            HighlightField field2 = readHighlightField(in);
            HighlightField field3 = readHighlightField(in);
            highlightFields = ImmutableMap.of(field1.name(), field1, field2.name(), field2, field3.name(), field3);
        } else if (size == 4) {
            HighlightField field1 = readHighlightField(in);
            HighlightField field2 = readHighlightField(in);
            HighlightField field3 = readHighlightField(in);
            HighlightField field4 = readHighlightField(in);
            highlightFields = ImmutableMap.of(field1.name(), field1, field2.name(), field2, field3.name(), field3,
                    field4.name(), field4);
        } else {
            ImmutableMap.Builder<String, HighlightField> builder = ImmutableMap.builder();
            for (int i = 0; i < size; i++) {
                HighlightField field = readHighlightField(in);
                builder.put(field.name(), field);
            }
            highlightFields = builder.build();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(type);
        if (highlightFields == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(highlightFields.size());
            for (HighlightField highlightField : highlightFields.values()) {
                highlightField.writeTo(out);
            }
        }
    }

}
