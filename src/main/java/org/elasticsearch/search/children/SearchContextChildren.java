/*
 * Licensed to Nicolas Lalevee under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
