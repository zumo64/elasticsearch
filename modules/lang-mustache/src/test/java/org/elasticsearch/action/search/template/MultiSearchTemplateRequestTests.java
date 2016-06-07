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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryParser;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.rest.action.search.template.RestMultiSearchTemplateAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchTemplateRequestTests extends ESTestCase {

    public void testParseRequest() throws Exception {
        IndicesQueriesRegistry registry = new IndicesQueriesRegistry();
        QueryParser<MatchAllQueryBuilder> parser = MatchAllQueryBuilder::fromXContent;
        registry.register(parser, MatchAllQueryBuilder.QUERY_NAME_FIELD);

        byte[] data = StreamsUtils.copyToBytesFromClasspath("/org/elasticsearch/action/search/template/simple-msearch-template.json");
        MultiSearchTemplateRequest request = RestMultiSearchTemplateAction.parseRequest(new MultiSearchTemplateRequest(),
                new BytesArray(data), null, null, null, null, IndicesOptions.strictExpandOpenAndForbidClosed(), true, registry,
                ParseFieldMatcher.EMPTY, null, null);

        assertThat(request.requests().size(), equalTo(3));
        assertThat(request.requests().get(0).getRequest().indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).indices(), arrayContaining("test0", "test1"));
        assertThat(request.requests().get(0).getRequest().requestCache(), equalTo(true));
        assertThat(request.requests().get(0).getRequest().preference(), nullValue());
        assertThat(request.requests().get(1).indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).getRequest().types()[0], equalTo("type1"));
        assertThat(request.requests().get(1).getRequest().requestCache(), nullValue());
        assertThat(request.requests().get(1).getRequest().preference(), equalTo("_local"));
        assertThat(request.requests().get(2).indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).getRequest().types()[0], equalTo("type2"));
        assertThat(request.requests().get(2).getRequest().types()[1], equalTo("type1"));
        assertThat(request.requests().get(2).getRequest().routing(), equalTo("123"));
        assertNotNull(request.requests().get(0).getScript());
        assertNotNull(request.requests().get(1).getScript());
        assertNotNull(request.requests().get(2).getScript());

        assertEquals(ScriptService.ScriptType.INLINE, request.requests().get(0).getScriptType());
        assertEquals(ScriptService.ScriptType.INLINE, request.requests().get(1).getScriptType());
        assertEquals(ScriptService.ScriptType.INLINE, request.requests().get(2).getScriptType());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(0).getScript());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(1).getScript());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(2).getScript());
        assertEquals(1, request.requests().get(0).getScriptParams().size());
        assertEquals(1, request.requests().get(1).getScriptParams().size());
        assertEquals(1, request.requests().get(2).getScriptParams().size());
    }
}
