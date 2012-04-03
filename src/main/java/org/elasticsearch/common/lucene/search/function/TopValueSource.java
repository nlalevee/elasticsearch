package org.elasticsearch.common.lucene.search.function;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.function.DocValues;
import org.apache.lucene.search.function.ValueSource;

/**
 * A value source that wraps another and ensures that the top level reader is used. This is useful for value sources
 * like ord() who's value depend on all those around it.
 */
public class TopValueSource extends ValueSource {
    private final ValueSource vs;

    public TopValueSource(ValueSource vs) {
        this.vs = vs;
    }

    public ValueSource getValueSource() {
        return vs;
    }

    @Override
    public String description() {
        return "top(" + vs.description() + ')';
    }

    @Override
    public DocValues getValues(IndexReader reader) throws IOException {
        return vs.getValues(reader);
    }

    @Override
    public boolean equals(Object o) {
        if (o.getClass() != TopValueSource.class)
            return false;
        TopValueSource other = (TopValueSource) o;
        return vs.equals(other.vs);
    }

    @Override
    public int hashCode() {
        int h = vs.hashCode();
        return (h << 1) | (h >>> 31);
    }

    @Override
    public String toString() {
        return "top(" + vs.toString() + ')';
    }
}