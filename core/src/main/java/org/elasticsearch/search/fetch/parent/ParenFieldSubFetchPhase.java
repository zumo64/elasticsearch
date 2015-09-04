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

package org.elasticsearch.search.fetch.parent;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.innerhits.InnerHitsContext;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.internal.ParentFieldMapper.*;

public class ParenFieldSubFetchPhase implements FetchSubPhase {

    @Override
    public Map<String, ? extends SearchParseElement> parseElements() {
        return Collections.emptyMap();
    }

    @Override
    public boolean hitExecutionNeeded(SearchContext context) {
        return context.fieldNames().contains(NAME) || hasParentChildInnerHits(context.innerHits());
    }

    private static boolean hasParentChildInnerHits(InnerHitsContext innerHitsContext) {
        if (innerHitsContext == null) {
            return false;
        }

        for (InnerHitsContext.BaseInnerHits baseInnerHits : innerHitsContext.getInnerHits().values()) {
            if (baseInnerHits instanceof InnerHitsContext.ParentChildInnerHits) {
                return true;
            }
        }
        return false;
    }


    @Override
    public void hitExecute(SearchContext context, HitContext hitContext) {
        ParentFieldMapper parentFieldMapper = context.mapperService().documentMapper(hitContext.hit().type()).parentFieldMapper();
        String internalFieldName = parentFieldMapper.useNewFieldName() ? parentFieldMapper.fieldType().names().indexName() : NAME;
        SingleFieldsVisitor fieldsVisitor = new SingleFieldsVisitor(internalFieldName);
        try {
            hitContext.reader().document(hitContext.docId(), fieldsVisitor);
            final List<Object> fieldValues;
            if (parentFieldMapper.useNewFieldName() == false) {
                fieldValues = fieldsVisitor.fields().get(internalFieldName);
                for (int i = 0; i < fieldValues.size(); i++) {
                    fieldValues.set(0, parentFieldMapper.fieldType().valueForSearch(fieldValues.get(0)));
                }
            } else {
                fieldsVisitor.postProcess(parentFieldMapper.fieldType());
                fieldValues = fieldsVisitor.fields().get(internalFieldName);
            }

            if (fieldValues != null) {
                InternalSearchHitField hitField = new InternalSearchHitField(NAME, fieldValues);
                Map<String, SearchHitField> fields = hitContext.hit().fieldsOrNull();
                if (fields == null) {
                    fields = new HashMap<>();
                    hitContext.hit().fields(fields);
                }
                fields.put(NAME, hitField);
            }
        } catch (IOException e) {
            throw ExceptionsHelper.convertToElastic(e);
        }
    }

    @Override
    public boolean hitsExecutionNeeded(SearchContext context) {
        return false;
    }

    @Override
    public void hitsExecute(SearchContext context, InternalSearchHit[] hits) {
    }
}
