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

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiDocValues.OrdinalMap;
import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.indices.breaker.CircuitBreakerService;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Utility class to build global ordinals.
 */
public enum GlobalOrdinalsBuilder {
    ;

    /**
     * Build global ordinals for the provided {@link IndexReader}.
     */
    public static IndexOrdinalsFieldData build(final IndexReader indexReader, IndexOrdinalsFieldData indexFieldData, Settings settings, CircuitBreakerService breakerService, ESLogger logger) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTimeNS = System.nanoTime();

        final AtomicOrdinalsFieldData[] atomicFD = new AtomicOrdinalsFieldData[indexReader.leaves().size()];
        final RandomAccessOrds[] subs = new RandomAccessOrds[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            atomicFD[i] = indexFieldData.load(indexReader.leaves().get(i));
            subs[i] = atomicFD[i].getOrdinalsValues();
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, subs, PackedInts.DEFAULT);
        final long memorySizeInBytes = ordinalMap.ramBytesUsed();
        breakerService.getBreaker(CircuitBreaker.FIELDDATA).addWithoutBreaking(memorySizeInBytes);

        if (logger.isDebugEnabled()) {
            logger.debug(
                    "Global-ordinals[{}][{}] took {} ms",
                    indexFieldData.getFieldNames().fullName(),
                    ordinalMap.getValueCount(),
                    TimeValue.nsecToMSec(System.nanoTime() - startTimeNS)
            );
        }
        return new InternalGlobalOrdinalsIndexFieldData(indexFieldData.index(), settings, indexFieldData.getFieldNames(),
                indexFieldData.getFieldDataType(), atomicFD, ordinalMap, memorySizeInBytes
        );
    }

    public static IndexOrdinalsFieldData buildEmpty(final IndexReader indexReader, IndexOrdinalsFieldData indexFieldData, Settings settings) throws IOException {
        assert indexReader.leaves().size() > 1;

        final AtomicOrdinalsFieldData[] atomicFD = new AtomicOrdinalsFieldData[indexReader.leaves().size()];
        final RandomAccessOrds[] subs = new RandomAccessOrds[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            atomicFD[i] = new AtomicOrdinalsFieldData() {
                @Override
                public RandomAccessOrds getOrdinalsValues() {
                    return DocValues.emptySortedSet();
                }

                @Override
                public ScriptDocValues getScriptValues() {
                    return new ScriptDocValues.Strings(getBytesValues());
                }

                @Override
                public SortedBinaryDocValues getBytesValues() {
                    return FieldData.toString(getOrdinalsValues());
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }

                @Override
                public Collection<Accountable> getChildResources() {
                    return Collections.emptyList();
                }

                @Override
                public void close() {
                }
            };
            subs[i] = atomicFD[i].getOrdinalsValues();
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, subs, PackedInts.DEFAULT);
        return new InternalGlobalOrdinalsIndexFieldData(indexFieldData.index(), settings, indexFieldData.getFieldNames(),
                indexFieldData.getFieldDataType(), atomicFD, ordinalMap, 0
        );
    }

}
