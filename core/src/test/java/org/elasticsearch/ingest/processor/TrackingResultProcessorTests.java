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

package org.elasticsearch.ingest.processor;

import org.elasticsearch.action.ingest.SimulateProcessorResult;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.ingest.core.CompoundProcessor.ON_FAILURE_MESSAGE_FIELD;
import static org.elasticsearch.ingest.core.CompoundProcessor.ON_FAILURE_PROCESSOR_TAG_FIELD;
import static org.elasticsearch.ingest.core.CompoundProcessor.ON_FAILURE_PROCESSOR_TYPE_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TrackingResultProcessorTests extends ESTestCase {

    private IngestDocument ingestDocument;
    private List<SimulateProcessorResult> resultList;

    @Before
    public void init() {
        ingestDocument = new IngestDocument(new HashMap<>(), new HashMap<>());
        resultList = new ArrayList<>();
    }

    public void testActualProcessor() throws Exception {
        TestProcessor actualProcessor = new TestProcessor(ingestDocument -> {});
        TrackingResultProcessor trackingProcessor = new TrackingResultProcessor(actualProcessor, resultList);
        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedResult = new SimulateProcessorResult(actualProcessor.getTag(), ingestDocument);

        assertThat(actualProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));

        assertThat(resultList.get(0).getIngestDocument(), equalTo(expectedResult.getIngestDocument()));
        assertThat(resultList.get(0).getFailure(), nullValue());
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithoutOnFailure() throws Exception {
        RuntimeException exception = new RuntimeException("processor failed");
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {  throw exception; });
        CompoundProcessor actualProcessor = new CompoundProcessor(testProcessor);
        CompoundProcessor trackingProcessor = TrackingResultProcessor.decorate(actualProcessor, resultList);

        try {
            trackingProcessor.execute(ingestDocument);
        } catch (Exception e) {
            assertThat(e.getMessage(), equalTo(exception.getMessage()));
        }

        SimulateProcessorResult expectedFirstResult = new SimulateProcessorResult(testProcessor.getTag(), ingestDocument);
        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(1));
        assertThat(resultList.get(0).getIngestDocument(), nullValue());
        assertThat(resultList.get(0).getFailure(), equalTo(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedFirstResult.getProcessorTag()));
    }

    public void testActualCompoundProcessorWithOnFailure() throws Exception {
        RuntimeException exception = new RuntimeException("fail");
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {  throw exception; });
        TestProcessor testOnFailureProcessor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor actualProcessor = new CompoundProcessor(Arrays.asList(testProcessor), Arrays.asList(testOnFailureProcessor));
        CompoundProcessor trackingProcessor = TrackingResultProcessor.decorate(actualProcessor, resultList);
        trackingProcessor.execute(ingestDocument);

        SimulateProcessorResult expectedFirstResult = new SimulateProcessorResult(testProcessor.getTag(), ingestDocument);
        SimulateProcessorResult expectedSecondResult = new SimulateProcessorResult(testOnFailureProcessor.getTag(), ingestDocument);

        IngestDocument ingestDocumentWithOnFailureMetadata = new IngestDocument(ingestDocument);
        Map<String, String> ingestMetadata = ingestDocumentWithOnFailureMetadata.getIngestMetadata();
        ingestMetadata.put(ON_FAILURE_MESSAGE_FIELD, exception.getMessage());
        ingestMetadata.put(ON_FAILURE_PROCESSOR_TYPE_FIELD, testProcessor.getType());
        ingestMetadata.put(ON_FAILURE_PROCESSOR_TAG_FIELD, testProcessor.getTag());

        assertThat(testProcessor.getInvokedCounter(), equalTo(1));
        assertThat(testOnFailureProcessor.getInvokedCounter(), equalTo(1));
        assertThat(resultList.size(), equalTo(2));
        assertThat(resultList.get(0).getIngestDocument(), nullValue());
        assertThat(resultList.get(0).getFailure(), equalTo(exception));
        assertThat(resultList.get(0).getProcessorTag(), equalTo(expectedFirstResult.getProcessorTag()));
        assertThat(resultList.get(1).getIngestDocument(), equalTo(ingestDocumentWithOnFailureMetadata));
        assertThat(resultList.get(1).getFailure(), nullValue());
        assertThat(resultList.get(1).getProcessorTag(), equalTo(expectedSecondResult.getProcessorTag()));
    }
}
