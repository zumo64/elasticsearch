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
import org.elasticsearch.action.search.template.MultiSearchTemplateAction;
import org.elasticsearch.action.search.template.MultiSearchTemplateRequest;
import org.elasticsearch.action.search.template.SearchTemplateRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.search.aggregations.AggregatorParsers;
import org.elasticsearch.search.suggest.Suggesters;

import java.util.Map;

import static org.elasticsearch.common.xcontent.support.XContentMapValues.lenientNodeBooleanValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringArrayValue;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.nodeStringValue;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestMultiSearchTemplateAction extends BaseRestHandler {

    private final boolean allowExplicitIndex;
    private final IndicesQueriesRegistry queryRegistry;
    private final AggregatorParsers aggParsers;
    private final Suggesters suggesters;

    @Inject
    public RestMultiSearchTemplateAction(Settings settings, RestController controller, Client client, IndicesQueriesRegistry queryRegistry,
                                         AggregatorParsers aggregatorParsers, Suggesters suggesters) {
        super(settings, client);
        this.queryRegistry = queryRegistry;
        this.aggParsers = aggregatorParsers;
        this.suggesters = suggesters;
        this.allowExplicitIndex = MULTI_ALLOW_EXPLICIT_INDEX.get(settings);

        controller.registerHandler(GET, "/_msearch/template", this);
        controller.registerHandler(POST, "/_msearch/template", this);
        controller.registerHandler(GET, "/{index}/_msearch/template", this);
        controller.registerHandler(POST, "/{index}/_msearch/template", this);
        controller.registerHandler(GET, "/{index}/{type}/_msearch/template", this);
        controller.registerHandler(POST, "/{index}/{type}/_msearch/template", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        if (RestActions.hasBodyContent(request) == false) {
            throw new ElasticsearchException("request body is required");
        }

        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();

        String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        String[] types = Strings.splitStringByCommaToArray(request.param("type"));
        IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, multiRequest.indicesOptions());
        String routing = request.param("routing");
        String searchType = request.param("search_type");

        parseRequest(multiRequest, RestActions.getRestContent(request), indices, types, searchType, routing, indicesOptions,
                allowExplicitIndex, queryRegistry, parseFieldMatcher, aggParsers, suggesters);

        client.execute(MultiSearchTemplateAction.INSTANCE, multiRequest, new RestToXContentListener<>(channel));
    }

    public static MultiSearchTemplateRequest parseRequest(MultiSearchTemplateRequest multiSearchRequest, BytesReference data,
                                                  @Nullable String[] indices,
                                                  @Nullable String[] types,
                                                  @Nullable String searchType,
                                                  @Nullable String routing,
                                                  IndicesOptions indicesOptions,
                                                  boolean allowExplicitIndex, IndicesQueriesRegistry indicesQueriesRegistry,
                                                  ParseFieldMatcher parseFieldMatcher, AggregatorParsers aggParsers,
                                                  Suggesters suggesters) throws Exception {

        XContent xContent = XContentFactory.xContent(data);
        int from = 0;
        int line = 0;
        int length = data.length();
        byte marker = xContent.streamSeparator();
        while (true) {
            int nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }
            line++;
            // support first line with \n
            if (nextMarker == 0) {
                from = nextMarker + 1;
                continue;
            }

            SearchRequest searchRequest = new SearchRequest();
            if (indices != null) {
                searchRequest.indices(indices);
            }
            if (indicesOptions != null) {
                searchRequest.indicesOptions(indicesOptions);
            }
            if (types != null && types.length > 0) {
                searchRequest.types(types);
            }
            if (routing != null) {
                searchRequest.routing(routing);
            }
            if (searchType != null) {
                searchRequest.searchType(searchType);
            }

            IndicesOptions defaultOptions = IndicesOptions.strictExpandOpenAndForbidClosed();

            // now parse the action
            if (nextMarker - from > 0) {
                try (XContentParser parser = xContent.createParser(data.slice(from, nextMarker - from))) {
                    Map<String, Object> source = parser.map();
                    for (Map.Entry<String, Object> entry : source.entrySet()) {
                        Object value = entry.getValue();
                        if ("index".equals(entry.getKey()) || "indices".equals(entry.getKey())) {
                            if (!allowExplicitIndex) {
                                throw new IllegalArgumentException("explicit index in multi search is not allowed");
                            }
                            searchRequest.indices(nodeStringArrayValue(value));
                        } else if ("type".equals(entry.getKey()) || "types".equals(entry.getKey())) {
                            searchRequest.types(nodeStringArrayValue(value));
                        } else if ("search_type".equals(entry.getKey()) || "searchType".equals(entry.getKey())) {
                            searchRequest.searchType(nodeStringValue(value, null));
                        } else if ("request_cache".equals(entry.getKey()) || "requestCache".equals(entry.getKey())) {
                            searchRequest.requestCache(lenientNodeBooleanValue(value));
                        } else if ("preference".equals(entry.getKey())) {
                            searchRequest.preference(nodeStringValue(value, null));
                        } else if ("routing".equals(entry.getKey())) {
                            searchRequest.routing(nodeStringValue(value, null));
                        }
                    }
                    defaultOptions = IndicesOptions.fromMap(source, defaultOptions);
                }
            }
            searchRequest.indicesOptions(defaultOptions);

            // move pointers
            from = nextMarker + 1;
            // now for the body
            nextMarker = findNextMarker(marker, from, data, length);
            if (nextMarker == -1) {
                break;
            }

            BytesReference slice = data.slice(from, nextMarker - from);
            SearchTemplateRequest searchTemplateRequest = RestSearchTemplateAction.parse(slice);
            if (searchTemplateRequest.getScript() != null) {
                searchTemplateRequest.setRequest(searchRequest);
                multiSearchRequest.add(searchTemplateRequest);
            } else {
                throw new IllegalArgumentException("Malformed line [" + line + "], expected a valid template");
            }

            // move pointers
            from = nextMarker + 1;
        }
        return multiSearchRequest;
    }

    private static int findNextMarker(byte marker, int from, BytesReference data, int length) {
        for (int i = from; i < length; i++) {
            if (data.get(i) == marker) {
                return i;
            }
        }
        return -1;
    }
}
