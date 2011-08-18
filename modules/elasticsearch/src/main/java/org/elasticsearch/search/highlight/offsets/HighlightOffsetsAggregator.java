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

package org.elasticsearch.search.highlight.offsets;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder;
import org.apache.lucene.search.vectorhighlight.FieldFragList;
import org.apache.lucene.search.vectorhighlight.FieldFragList.WeightedFragInfo;
import org.apache.lucene.search.vectorhighlight.FieldPhraseList;
import org.apache.lucene.search.vectorhighlight.FieldQuery;
import org.apache.lucene.search.vectorhighlight.FieldTermStack;
import org.apache.lucene.search.vectorhighlight.FragListBuilder;
import org.elasticsearch.search.highlight.SearchContextHighlight;

/**
 * The pending of the FastVectorHighlighter but here only consider offsets, does not build any actual fragment
 */
public class HighlightOffsetsAggregator {

    public HighlightOffsets[] getBestHighlightOffsets(SearchContextHighlight.Field field, IndexReader reader,
            int docId, String fieldName, FieldQuery fieldQuery, BaseFragmentsBuilder fragmentsBuilder,
            FragListBuilder fragListBuilder) throws IOException {
        FieldFragList fieldFragList = getFieldFragList(fragListBuilder, fieldQuery, reader, docId, fieldName, docId);

        // code inspired from org.apache.lucene.search.vectorhighlight.BaseFragmentsBuilder.createFragments(IndexReader,
        // int, String, FieldFragList, int, String[], String[], Encoder)

        List<WeightedFragInfo> fragInfos = fragmentsBuilder.getWeightedFragInfoList(fieldFragList.getFragInfos());

        List<HighlightOffsets> fragments = new ArrayList<HighlightOffsets>();
        for (int n = 0; n < field.numberOfFragments() && n < fragInfos.size(); n++) {
            WeightedFragInfo fragInfo = fragInfos.get(n);
            fragments.add(new HighlightOffsets(fragInfo.getStartOffset(), fragInfo.getEndOffset()));
        }
        return fragments.toArray(new HighlightOffsets[fragments.size()]);
    }

    private FieldFragList getFieldFragList(FragListBuilder fragListBuilder, final FieldQuery fieldQuery,
            IndexReader reader, int docId, String fieldName, int fragCharSize) throws IOException {
        FieldTermStack fieldTermStack = new FieldTermStack(reader, docId, fieldName, fieldQuery);
        FieldPhraseList fieldPhraseList = new FieldPhraseList(fieldTermStack, fieldQuery);
        return fragListBuilder.createFieldFragList(fieldPhraseList, fragCharSize);
    }
}
