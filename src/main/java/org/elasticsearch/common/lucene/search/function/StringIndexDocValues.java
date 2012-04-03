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

package org.elasticsearch.common.lucene.search.function;

import java.io.IOException;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.FieldCache;
import org.apache.lucene.search.function.DocValues;
import org.apache.lucene.search.function.ValueSource;

/**
 * Internal class, subject to change. Serves as base class for DocValues based on StringIndex
 **/
public abstract class StringIndexDocValues extends DocValues {
    protected final FieldCache.StringIndex index;
    protected final int[] order;
    protected final String[] lookup;
    protected final ValueSource vs;

    public StringIndexDocValues(ValueSource vs, IndexReader reader, String field) throws IOException {
        try {
            index = FieldCache.DEFAULT.getStringIndex(reader, field);
        } catch (RuntimeException e) {
            throw new StringIndexException(field, e);
        }
        order = index.order;
        lookup = index.lookup;
        this.vs = vs;
    }

    protected abstract String toTerm(String readableValue);

    @Override
    public String toString(int doc) {
        return vs.description() + '=' + strVal(doc);
    }

    public static final class StringIndexException extends RuntimeException {
        public StringIndexException(final String fieldName, final RuntimeException cause) {
            super("Can't initialize StringIndex to generate (function) " + "DocValues for field: " + fieldName, cause);
        }
    }

}
