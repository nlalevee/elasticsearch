/*
 * Licensed to Elastic Search and Shay Banon under one
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

package org.elasticsearch.search.highlight;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.search.vectorhighlight.*;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.io.FastStringReader;
import org.elasticsearch.common.lucene.document.SingleFieldSelector;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchPhaseExecutionException;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.highlight.offsets.HighlightOffsets;
import org.elasticsearch.search.highlight.offsets.HighlightOffsetsAggregator;
import org.elasticsearch.search.highlight.offsets.NopFormatter;
import org.elasticsearch.search.highlight.vectorhighlight.SourceScoreOrderFragmentsBuilder;
import org.elasticsearch.search.highlight.vectorhighlight.SourceSimpleFragmentsBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Maps.*;

/**
 * @author kimchy (shay.banon)
 */
public class HighlightPhase implements FetchSubPhase {

    public static class Encoders {
        public static Encoder DEFAULT = new DefaultEncoder();
        public static Encoder HTML = new SimpleHTMLEncoder();
    }

    @Override public Map<String, ? extends SearchParseElement> parseElements() {
        return ImmutableMap.of("highlight", new HighlighterParseElement());
    }

    @Override public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override public void hitsExecute(SearchContext context, InternalSearchHit[] hits) throws ElasticSearchException {
    }

    @Override public boolean hitExecutionNeeded(SearchContext context) {
        return context.highlight() != null;
    }

    @Override public void hitExecute(SearchContext context, HitContext hitContext) throws ElasticSearchException {
        try {
            DocumentMapper documentMapper = context.mapperService().documentMapper(hitContext.hit().type());

            boolean offsetsOnly = context.highlight().offsetsOnly();

            Map<String, HighlightField> highlightFields = null;
            Map<String, HighlightOffsets[]> highlightOffsets = null;
            if (offsetsOnly) {
                highlightOffsets = newHashMap();
            } else {
                highlightFields = newHashMap();
            }
            for (SearchContextHighlight.Field field : context.highlight().fields()) {
                Encoder encoder;
                if (field.encoder().equals("html")) {
                    encoder = Encoders.HTML;
                } else {
                    encoder = Encoders.DEFAULT;
                }
                FieldMapper mapper = documentMapper.mappers().smartNameFieldMapper(field.field());
                if (mapper == null) {
                    MapperService.SmartNameFieldMappers fullMapper = context.mapperService().smartName(field.field());
                    if (fullMapper == null || !fullMapper.hasDocMapper()) {
                        //Save skipping missing fields
                        continue;
                    }
                    if (!fullMapper.docMapper().type().equals(hitContext.hit().type())) {
                        continue;
                    }
                    mapper = fullMapper.mapper();
                    if (mapper == null) {
                        continue;
                    }
                }

                // if we can do highlighting using Term Vectors, use FastVectorHighlighter, otherwise, use the
                // slower plain highlighter
                if (mapper.termVector() != Field.TermVector.WITH_POSITIONS_OFFSETS) {
                    // Don't use the context.query() since it might be rewritten, and we need to pass the non rewritten queries to
                    // let the highlighter handle MultiTerm ones

                    // QueryScorer uses WeightedSpanTermExtractor to extract terms, but we can't really plug into
                    // it, so, we hack here (and really only support top level queries)
                    Query query = context.parsedQuery().query();
                    if (query instanceof FunctionScoreQuery) {
                        query = ((FunctionScoreQuery) query).getSubQuery();
                    } else if (query instanceof FiltersFunctionScoreQuery) {
                        query = ((FiltersFunctionScoreQuery) query).getSubQuery();
                    } else if (query instanceof ConstantScoreQuery) {
                        ConstantScoreQuery q = (ConstantScoreQuery) query;
                        if (q.getQuery() != null) {
                            query = q.getQuery();
                        }
                    }
                    QueryScorer queryScorer = new QueryScorer(query, null);
                    queryScorer.setExpandMultiTermQuery(true);
                    Fragmenter fragmenter;
                    if (field.numberOfFragments() == 0) {
                        fragmenter = new NullFragmenter();
                    } else {
                        int fragmentCharSize;
                        if (offsetsOnly) {
                            // we don't care about fragment merging
                            fragmentCharSize = 0;
                        } else {
                            fragmentCharSize = field.fragmentCharSize();
                        }
                        fragmenter = new SimpleSpanFragmenter(queryScorer, fragmentCharSize);
                    }
                    Formatter formatter;
                    if (offsetsOnly) {
                        formatter = NopFormatter.INSTANCE;
                    } else {
                        formatter = new SimpleHTMLFormatter(field.preTags()[0], field.postTags()[0]);
                    }

                    Highlighter highlighter = new Highlighter(formatter, encoder, queryScorer);
                    highlighter.setTextFragmenter(fragmenter);

                    List<Object> textsToHighlight;
                    if (mapper.stored()) {
                        try {
                            Document doc = hitContext.reader().document(hitContext.docId(), new SingleFieldSelector(mapper.names().indexName()));
                            textsToHighlight = new ArrayList<Object>(doc.getFields().size());
                            for (Fieldable docField : doc.getFields()) {
                                if (docField.stringValue() != null) {
                                    textsToHighlight.add(docField.stringValue());
                                }
                            }
                        } catch (Exception e) {
                            throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                        }
                    } else {
                        SearchLookup lookup = context.lookup();
                        lookup.setNextReader(hitContext.reader());
                        lookup.setNextDocId(hitContext.docId());
                        textsToHighlight = lookup.source().extractRawValues(mapper.names().fullName());
                    }

                    // a HACK to make highlighter do highlighting, even though its using the single frag list builder
                    int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                    ArrayList<TextFragment> fragsList = new ArrayList<TextFragment>();
                    try {
                        int globalOffset = 0;
                        for (Object textToHighlight : textsToHighlight) {
                            String text = textToHighlight.toString();
                            Analyzer analyzer = context.mapperService().documentMapper(hitContext.hit().type()).mappers().indexAnalyzer();
                            TokenStream tokenStream = analyzer.reusableTokenStream(mapper.names().indexName(), new FastStringReader(text));
                            TextFragment[] bestTextFragments = highlighter.getBestTextFragments(tokenStream, text, false, numberOfFragments);
                            for (TextFragment bestTextFragment : bestTextFragments) {
                                if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
                                    if (offsetsOnly) {
                                        int start = TextFragmentAccessor.getTextStartPos(bestTextFragment);
                                        int end = TextFragmentAccessor.getTextStartPos(bestTextFragment);
                                        TextFragmentAccessor.setTextStartPos(bestTextFragment, start + globalOffset);
                                        TextFragmentAccessor.setTextEndPos(bestTextFragment, end + globalOffset);
                                    }
                                    fragsList.add(bestTextFragment);
                                }
                            }
                            globalOffset += text.length();
                        }
                    } catch (Exception e) {
                        throw new FetchPhaseExecutionException(context, "Failed to highlight field [" + field.field() + "]", e);
                    }
                    if (field.scoreOrdered()) {
                        Collections.sort(fragsList, new Comparator<TextFragment>() {
                            public int compare(TextFragment o1, TextFragment o2) {
                                return Math.round(o2.getScore() - o1.getScore());
                            }
                        });
                    }

                    if (offsetsOnly) {
                        if (!fragsList.isEmpty()) {
                            if (field.scoreOrdered()) {
                                Collections.sort(fragsList, new Comparator<TextFragment>() {
                                    public int compare(TextFragment o1, TextFragment o2) {
                                        return Math.round(o2.getScore() - o1.getScore());
                                    }
                                });
                            }

                            numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size()
                                    : numberOfFragments;

                            HighlightOffsets[] offsets = new HighlightOffsets[numberOfFragments];
                            for (int i = 0; i < offsets.length; i++) {
                                int start = TextFragmentAccessor.getTextStartPos(fragsList.get(i));
                                int end = TextFragmentAccessor.getTextEndPos(fragsList.get(i));
                                offsets[i] = new HighlightOffsets(start, end);
                            }

                            highlightOffsets.put(field.field(), offsets);
                        }
                    } else {
                        String[] fragments = null;
                        // number_of_fragments is set to 0 but we have a multivalued field
                        if (field.numberOfFragments() == 0 && textsToHighlight.size() > 1 && fragsList.size() > 0) {
                            fragments = new String[1];
                            for (int i = 0; i < fragsList.size(); i++) {
                                fragments[0] = (fragments[0] != null ? (fragments[0] + " ") : "")
                                        + fragsList.get(i).toString();
                            }
                        } else {
                            // refine numberOfFragments if needed
                            numberOfFragments = fragsList.size() < numberOfFragments ? fragsList.size()
                                    : numberOfFragments;
                            fragments = new String[numberOfFragments];
                            for (int i = 0; i < fragments.length; i++) {
                                fragments[i] = fragsList.get(i).toString();
                            }
                        }

                        if (fragments != null && fragments.length > 0) {
                            HighlightField highlightField = new HighlightField(field.field(), fragments);
                            highlightFields.put(highlightField.name(), highlightField);
                        }
                    }
                } else {
                    boolean phraseHighlight = true;
                    boolean fieldMatch = false;

                    FragListBuilder fragListBuilder = buildFragListBuilder(field);
                    BaseFragmentsBuilder fragmentsBuilder = buildFragmentsBuilder(context, mapper, field);

                    FieldQuery fieldQuery = buildFieldQuery(phraseHighlight, fieldMatch, context.parsedQuery().query(),
                            hitContext.reader(), field);

                    if (offsetsOnly) {
                        HighlightOffsetsAggregator aggregator = new HighlightOffsetsAggregator();
                        HighlightOffsets[] offsets;
                        try {
                            offsets = aggregator.getBestHighlightOffsets(field, hitContext.reader(),
                                    hitContext.docId(), field.field(), fieldQuery, fragmentsBuilder, fragListBuilder);
                        } catch (IOException e) {
                            throw new FetchPhaseExecutionException(context, "Failed to highlight field ["
                                    + field.field() + "]", e);
                        }
                        highlightOffsets.put(field.field(), offsets);
                    } else {
                        FastVectorHighlighter highlighter = new FastVectorHighlighter(phraseHighlight, fieldMatch,
                                fragListBuilder, fragmentsBuilder);
                        String[] fragments;
                        try {
                            // a HACK to make highlighter do highlighting, even though its using the single frag list
                            // builder
                            int numberOfFragments = field.numberOfFragments() == 0 ? 1 : field.numberOfFragments();
                            fragments = highlighter.getBestFragments(fieldQuery, hitContext.reader(),
                                    hitContext.docId(), mapper.names().indexName(), field.fragmentCharSize(),
                                    numberOfFragments,
                                    fragListBuilder, fragmentsBuilder, field.preTags(), field.postTags(), encoder);
                        } catch (IOException e) {
                            throw new FetchPhaseExecutionException(context, "Failed to highlight field ["
                                    + field.field() + "]", e);
                        }
                        if (fragments != null && fragments.length > 0) {
                            HighlightField highlightField = new HighlightField(field.field(), fragments);
                            highlightFields.put(highlightField.name(), highlightField);
                        }
                    }
                }
            }

            hitContext.hit().highlightFields(highlightFields);
            hitContext.hit().highlightOffsets(highlightOffsets);
        } finally {
            CustomFieldQuery.reader.remove();
            CustomFieldQuery.highlightFilters.remove();
        }
    }

    private FieldQuery buildFieldQuery(boolean phraseHighlight, boolean fieldMatch, Query query,
            IndexReader indexReader, SearchContextHighlight.Field field) {
        CustomFieldQuery.reader.set(indexReader);
        CustomFieldQuery.highlightFilters.set(field.highlightFilter());
        return new CustomFieldQuery(query, phraseHighlight, phraseHighlight);
    }

    private FragListBuilder buildFragListBuilder(SearchContextHighlight.Field field) {
        FragListBuilder fragListBuilder;
        if (field.numberOfFragments() == 0) {
            fragListBuilder = new SingleFragListBuilder();
        } else if (field.fragmentOffset() == -1) {
            fragListBuilder = new SimpleFragListBuilder();
        } else {
            fragListBuilder = new MarginFragListBuilder(field.fragmentOffset());
        }

        return fragListBuilder;
    }

    private BaseFragmentsBuilder buildFragmentsBuilder(SearchContext searchContext, FieldMapper fieldMapper,
            SearchContextHighlight.Field field) {
        BaseFragmentsBuilder fragmentsBuilder;
        if (field.numberOfFragments() == 0) {
            if (fieldMapper.stored()) {
                fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
            } else {
                fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldMapper, searchContext, field.preTags(),
                        field.postTags());
            }
        } else {
            if (field.scoreOrdered()) {
                if (fieldMapper.stored()) {
                    fragmentsBuilder = new ScoreOrderFragmentsBuilder(field.preTags(), field.postTags());
                } else {
                    fragmentsBuilder = new SourceScoreOrderFragmentsBuilder(fieldMapper, searchContext,
                            field.preTags(), field.postTags());
                }
            } else {
                if (fieldMapper.stored()) {
                    fragmentsBuilder = new SimpleFragmentsBuilder(field.preTags(), field.postTags());
                } else {
                    fragmentsBuilder = new SourceSimpleFragmentsBuilder(fieldMapper, searchContext, field.preTags(),
                            field.postTags());
                }
            }
        }

        return fragmentsBuilder;
    }
}
