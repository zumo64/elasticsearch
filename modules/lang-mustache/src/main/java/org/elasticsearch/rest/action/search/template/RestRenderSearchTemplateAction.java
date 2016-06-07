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

import org.elasticsearch.action.search.template.RenderSearchTemplateAction;
import org.elasticsearch.action.search.template.RenderSearchTemplateRequest;
import org.elasticsearch.action.search.template.RenderSearchTemplateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.support.RestActions;
import org.elasticsearch.rest.action.support.RestBuilderListener;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;

import static org.elasticsearch.action.search.template.RenderSearchTemplateRequest.fromXContent;
import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.RestStatus.OK;

public class RestRenderSearchTemplateAction extends BaseRestHandler {

    @Inject
    public RestRenderSearchTemplateAction(Settings settings, RestController controller, Client client) {
        super(settings, client);
        controller.registerHandler(GET, "/_render/template", this);
        controller.registerHandler(POST, "/_render/template", this);
        controller.registerHandler(GET, "/_render/template/{id}", this);
        controller.registerHandler(POST, "/_render/template/{id}", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {
        // Creates the render template request
        RenderSearchTemplateRequest renderRequest = parseRequest(request);

        client.execute(RenderSearchTemplateAction.INSTANCE, renderRequest, new RestBuilderListener<RenderSearchTemplateResponse>(channel) {
            @Override
            public RestResponse buildResponse(RenderSearchTemplateResponse response, XContentBuilder builder) throws Exception {
                response.toXContent(builder.prettyPrint(), ToXContent.EMPTY_PARAMS);
                return new BytesRestResponse(OK, builder);
            }
        });
    }

    static RenderSearchTemplateRequest parseRequest(BytesReference bytes) throws IOException {
        return fromXContent(bytes, new RenderSearchTemplateRequest());
    }

    static RenderSearchTemplateRequest parseRequest(RestRequest request) throws IOException {
        RenderSearchTemplateRequest renderRequest = parseRequest(RestActions.getRestContent(request));
        String id = request.param("id");
        if (id != null) {
            renderRequest.setScriptType(ScriptService.ScriptType.STORED);
            renderRequest.setScript(id);
        }
        return renderRequest;
    }
}
