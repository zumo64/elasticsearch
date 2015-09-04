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

import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.*;

/**
 * A query that will return only documents matching specific parent ids for a particular type.
 */
public class ParentIdQueryBuilder extends QueryBuilder implements BoostableQueryBuilder<ParentIdQueryBuilder> {

    private final String childType;
    private final List<String> parentIds = new ArrayList<>();
    private float boost = -1;
    private String queryName;

    public ParentIdQueryBuilder(String childType, String parentId) {
        Objects.requireNonNull(childType, "type is a required parameter");
        Objects.requireNonNull(parentId, "parentId is a required parameter");

        this.childType = childType;
        this.parentIds.add(parentId);
    }

    /**
     * Adds a parent id.
     */
    public ParentIdQueryBuilder addParentId(String parentId) {
        parentIds.add(parentId);
        return this;
    }

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     */
    @Override
    public ParentIdQueryBuilder boost(float boost) {
        this.boost = boost;
        return this;
    }

    /**
     * Sets the query name for the filter that can be used when searching for matched_filters per hit.
     */
    public ParentIdQueryBuilder queryName(String queryName) {
        this.queryName = queryName;
        return this;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(ParentIdQueryParser.NAME);
        builder.field("type", childType);
        builder.array("ids", parentIds.toArray());
        if (boost != -1) {
            builder.field("boost", boost);
        }
        if (queryName != null) {
            builder.field("_name", queryName);
        }
        builder.endObject();
    }
}