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

package org.elasticsearch.rest.action.search.template;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.template.SearchTemplateAction;
import org.elasticsearch.action.search.template.SearchTemplateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestSearchTemplateAction extends BaseRestHandler {

    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggParsers;
    private final Suggesters suggesters;

    @Inject
    public RestSearchTemplateAction(Settings settings, RestController controller, Client client, IndicesQueriesRegistry queryRegistry,
                                    AggregatorParsers aggregatorParsers, Suggesters suggesters) {
        super(settings, client);
        this.queryRegistry = queryRegistry;
        this.aggParsers = aggregatorParsers;
        this.suggesters = suggesters;

        controller.registerHandler(GET, "/_search/template", this);
        controller.registerHandler(POST, "/_search/template", this);
        controller.registerHandler(GET, "/{index}/_search/template", this);
        controller.registerHandler(POST, "/{index}/_search/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_search/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_search/template", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        if (RestActions.hasBodyContent(request) == false) {
            throw new ElasticsearchException("request body is required");
        }

        // Creates the search request with all required params
        SearchRequest searchRequest = new SearchRequest();
        RestSearchAction.parseSearchRequest(searchRequest, queryRegistry, request, parseFieldMatcher, aggParsers, suggesters, null);

        // Creates the search template request
        SearchTemplateRequest searchTemplateRequest = SearchTemplateRequest.parse(RestActions.getRestContent(request));
        searchTemplateRequest.setRequest(searchRequest);

        client.execute(SearchTemplateAction.INSTANCE, searchTemplateRequest, new RestToXContentListener<>(channel));
    }
}
