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

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.script.ScriptService;

import java.util.Map;

public abstract class AbstractSearchTemplateRequestBuilder<Request extends AbstractSearchTemplateRequest<Request>,
        Response extends ActionResponse,
        RequestBuilder extends AbstractSearchTemplateRequestBuilder<Request, Response, RequestBuilder>>
        extends ActionRequestBuilder<Request, Response, RequestBuilder> {

    protected AbstractSearchTemplateRequestBuilder(ElasticsearchClient client,
                                                   Action<Request, Response, RequestBuilder> action,
                                                   Request request) {
        super(client, action, request);
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder setScriptType(ScriptService.ScriptType scriptType) {
        request.setScriptType(scriptType);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder setScript(String script) {
        request.setScript(script);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public RequestBuilder setScriptParams(Map<String, Object> scriptParams) {
        request.setScriptParams(scriptParams);
        return (RequestBuilder) this;
    }
}
