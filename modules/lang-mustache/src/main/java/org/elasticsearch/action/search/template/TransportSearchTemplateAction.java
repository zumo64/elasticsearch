/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.action.search.template;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.ScriptContext.Standard.SEARCH;

public class TransportSearchTemplateAction extends HandledTransportAction<SearchTemplateRequest, SearchTemplateResponse> {

    private static final String TEMPLATE_LANG = MustacheScriptEngineService.NAME;

    private final ClusterService clusterService;
    private final ScriptService scriptService;
    private final TransportSearchAction searchAction;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggsParsers;
    private final Suggesters suggesters;

    @Inject
    public TransportSearchTemplateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver resolver,
                                         ClusterService clusterService, ScriptService scriptService,
                                         TransportSearchAction searchAction, IndicesQueriesRegistry indicesQueryRegistry,
                                         AggregatorParsers aggregatorParsers, Suggesters suggesters) {
        super(settings, SearchTemplateAction.NAME, threadPool, transportService, actionFilters, resolver, SearchTemplateRequest::new);
        this.clusterService = clusterService;
        this.scriptService = scriptService;
        this.searchAction = searchAction;
        this.queryRegistry = indicesQueryRegistry;
        this.aggsParsers = aggregatorParsers;
        this.suggesters = suggesters;
    }


    @Override
    protected void doExecute(SearchTemplateRequest request, ActionListener<SearchTemplateResponse> listener) {
        new AsyncSearchTemplateAction(request, listener, SEARCH, clusterService.state()).start();
    }

    class AsyncSearchTemplateAction {

        private final SearchTemplateRequest request;
        private final SearchTemplateResponse response;
        private final ActionListener<SearchTemplateResponse> listener;
        private final ScriptContext scriptContext;
        private final ClusterState clusterState;

        public AsyncSearchTemplateAction(SearchTemplateRequest request, ActionListener<SearchTemplateResponse> listener,
                                         ScriptContext scriptContext, ClusterState clusterState) {
            this.request = Objects.requireNonNull(request);
            this.listener = Objects.requireNonNull(listener);
            this.scriptContext = Objects.requireNonNull(scriptContext);
            this.clusterState = Objects.requireNonNull(clusterState);
            this.response = new SearchTemplateResponse();
        }

        void start() {
            try {
                Script script = new Script(request.getScript(), request.getScriptType(), TEMPLATE_LANG, request.getScriptParams());
                ExecutableScript executable = scriptService.executable(script, scriptContext, emptyMap(), clusterState);

                BytesReference source = (BytesReference) executable.run();
                response.setSource(source);

                if (request.isSimulate() == false) {
                    SearchRequest searchRequest = request.getRequest();

                    try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
                        SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
                        builder.parseXContent(new QueryParseContext(queryRegistry, parser, parseFieldMatcher), aggsParsers, suggesters);
                        searchRequest.source(builder);

                        searchAction.execute(searchRequest, new ActionListener<SearchResponse>() {
                            @Override
                            public void onResponse(SearchResponse searchResponse) {
                                onSearchResponse(searchResponse);
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                onSearchFailure(e);
                            }
                        });
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                } else {
                    finishHim();
                }
            } catch (Throwable t) {
                listener.onFailure(t);
            }
        }

        private void onSearchResponse(SearchResponse searchResponse) {
            response.setResponse(searchResponse);
            finishHim();
        }

        private void onSearchFailure(Throwable e) {
            listener.onFailure(e);
        }

        void finishHim() {
            try {
                listener.onResponse(response);
            } catch (Throwable e) {
                listener.onFailure(e);
            }
        }
    }
}
