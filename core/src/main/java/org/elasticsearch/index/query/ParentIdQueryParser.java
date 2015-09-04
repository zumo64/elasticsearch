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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParentIdQueryParser implements QueryParser {

    public static final String NAME = "parent_id";

    @Inject
    public ParentIdQueryParser() {
    }

    @Override
    public String[] names() {
        return new String[]{NAME};
    }

    @Override
    public Query parse(QueryParseContext parseContext) throws IOException, QueryParsingException {
        XContentParser parser = parseContext.parser();

        String childType = null;
        List<String> parentIds = new ArrayList<>();

        String currentFieldName = null;
        float boost = 1.0f;
        String queryName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_ARRAY) {
                if ("ids".equals(currentFieldName)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token.isValue()) {
                            parentIds.add(parser.text());
                        } else {
                            throw new QueryParsingException(parseContext, "unexpected token [{}]", token);
                        }
                    }
                } else {
                    throw new QueryParsingException(parseContext, "unexpected field [{}]", currentFieldName);
                }
            } else  if (token.isValue()) {
                if ("type".equals(currentFieldName)) {
                    childType = parser.text();
                } else if ("id".equals(currentFieldName)) {
                    parentIds.add(parser.text());
                }else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else {
                    throw new QueryParsingException(parseContext, "unexpected field [{}]", currentFieldName);
                }
            } else {
                throw new QueryParsingException(parseContext, "unexpected token [{}]", token);
            }
        }

        if (childType == null) {
            throw new QueryParsingException(parseContext, "couldn't resolve type");
        }

        if (parentIds.isEmpty()) {
            throw new QueryParsingException(parseContext, "no parent ids specified");
        }

        DocumentMapper documentMapper = parseContext.mapperService().documentMapper(childType);
        if (documentMapper == null) {
            throw new QueryParsingException(parseContext, "type [{}] doesn't exist", childType);
        }
        ParentFieldMapper parentFieldMapper = documentMapper.parentFieldMapper();
        if (parentFieldMapper.active() == false) {
            throw new QueryParsingException(parseContext, "type [{}] doesn't have a _parent field configured", childType);
        }

        Query query = parentFieldMapper.fieldType().termsQuery(parentIds, parseContext);
        query.setBoost(boost);
        if (queryName != null) {
            parseContext.addNamedQuery(queryName, query);
        }
        return query;
    }
}
