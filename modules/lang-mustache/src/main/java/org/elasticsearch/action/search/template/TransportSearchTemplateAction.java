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
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggesters;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportSearchTemplateAction extends AbstractSearchTemplateAction<SearchTemplateRequest, SearchResponse> {

    private final TransportSearchAction searchAction;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggregatorParsers;
    private final Suggesters suggesters;

    @Inject
    public TransportSearchTemplateAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         ClusterService clusterService, ScriptService scriptService,
                                         TransportSearchAction searchAction, IndicesQueriesRegistry indicesQueryRegistry,
                                         AggregatorParsers aggregatorParsers, Suggesters suggesters) {
        super(settings, SearchTemplateAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                SearchTemplateRequest::new, clusterService, scriptService);
        this.searchAction = searchAction;
        this.queryRegistry = indicesQueryRegistry;
        this.aggregatorParsers = aggregatorParsers;
        this.suggesters = suggesters;
    }

    @Override
    protected void doExecute(SearchTemplateRequest request, ActionListener<SearchResponse> listener, BytesReference source) {
        try (XContentParser parser = XContentFactory.xContent(source).createParser(source)) {
            SearchSourceBuilder builder = SearchSourceBuilder.searchSource();
            builder.parseXContent(new QueryParseContext(queryRegistry, parser, parseFieldMatcher), aggregatorParsers, suggesters);
            request.getRequest().source(builder);

            searchAction.execute(request.getRequest(), listener);
        } catch (IOException e) {
            listener.onFailure(e);
        }
    }
}
