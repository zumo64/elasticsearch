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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.script.ScriptContext.Standard.SEARCH;

public abstract class AbstractSearchTemplateAction<Request extends AbstractSearchTemplateRequest<Request>, Response extends ActionResponse>
        extends HandledTransportAction<Request, Response> {

    private static final String TEMPLATE_LANG = MustacheScriptEngineService.NAME;

    private final ClusterService clusterService;
    private final ScriptService scriptService;

    public AbstractSearchTemplateAction(Settings settings, String actionName, ThreadPool threadPool, TransportService transportService,
                                        ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                        Supplier<Request> request, ClusterService clusterService, ScriptService scriptService) {
        super(settings, actionName, threadPool, transportService, actionFilters, indexNameExpressionResolver, request);
        this.clusterService = clusterService;
        this.scriptService = scriptService;
    }

    @Override
    protected void doExecute(Request request, ActionListener<Response> listener) {
        new AsyncSearchTemplateAction(request, listener).start();
    }

    /**
     * Execute the action when the search template is rendered
     */
    protected abstract void doExecute(Request request, ActionListener<Response> listener, BytesReference source);

    class AsyncSearchTemplateAction {

        private final Request request;
        private final ActionListener<Response> listener;

        public AsyncSearchTemplateAction(Request request, ActionListener<Response> listener) {
            this.request = request;
            this.listener = listener;
        }

        void start() {
            try {
                Script script = new Script(request.getScript(), request.getScriptType(), TEMPLATE_LANG, request.getScriptParams());
                ExecutableScript executable = scriptService.executable(script, SEARCH, emptyMap(), clusterService.state());
                doExecute(request, listener, (BytesReference) executable.run());
            } catch (Throwable t) {
                listener.onFailure(t);
            }
        }
    }
}
