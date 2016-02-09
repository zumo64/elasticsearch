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
import org.elasticsearch.ingest.core.CompoundProcessor;
import org.elasticsearch.ingest.core.IngestDocument;
import org.elasticsearch.ingest.core.Processor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Processor to be used within Simulate API to keep track of processors executed in pipeline.
 */
public final class TrackingResultProcessor implements Processor {

    private final Processor actualProcessor;
    private final List<SimulateProcessorResult> processorResultList;

    public TrackingResultProcessor(Processor actualProcessor, List<SimulateProcessorResult> processorResultList) {
        this.processorResultList = processorResultList;
        if (actualProcessor instanceof CompoundProcessor) {
            CompoundProcessor actualCompoundProcessor = (CompoundProcessor) actualProcessor;
            List<Processor> trackedProcessors = actualCompoundProcessor.getProcessors().stream()
                    .map(p -> new TrackingResultProcessor(p, processorResultList))
                    .collect(Collectors.toList());
            List<Processor> trackedOnFailureProcessors = actualCompoundProcessor.getOnFailureProcessors().stream()
                    .map(p -> new TrackingResultProcessor(p, processorResultList)).collect(Collectors.toList());
            CompoundProcessor trackedCompoundProcessor = new CompoundProcessor(trackedProcessors,trackedOnFailureProcessors);
            this.actualProcessor = trackedCompoundProcessor;
        } else {
            this.actualProcessor = actualProcessor;
        }
    }

    @Override
    public void execute(IngestDocument ingestDocument) throws Exception {
        if (actualProcessor instanceof CompoundProcessor) {
            actualProcessor.execute(ingestDocument);
        } else {
            try {
                actualProcessor.execute(ingestDocument);
                processorResultList.add(new SimulateProcessorResult(actualProcessor.getTag(), new IngestDocument(ingestDocument)));
            } catch (Exception e) {
                processorResultList.add(new SimulateProcessorResult(actualProcessor.getTag(), e));
                throw e;
            }
        }
    }

    @Override
    public String getType() {
        return actualProcessor.getType();
    }

    @Override
    public String getTag() {
        return actualProcessor.getTag();
    }
}

