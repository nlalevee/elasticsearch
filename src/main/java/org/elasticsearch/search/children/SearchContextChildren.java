package org.elasticsearch.search.children;

import java.util.List;

import org.elasticsearch.search.highlight.SearchContextHighlight.Field;

public class SearchContextChildren {

    private final List<Field> fields;

    private final int size;

    public SearchContextChildren(int size, List<Field> fields) {
        this.size = size;
        this.fields = fields;
    }

    public List<Field> fields() {
        return fields;
    }

    public int size() {
        return size;
    }

}
