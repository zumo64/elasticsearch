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

package org.elasticsearch.ingest;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class PipelineExecutionService implements ClusterStateListener {

    private final PipelineStore store;
    private final ThreadPool threadPool;

    private final StatsHolder totalStats = new StatsHolder();
    private volatile Map<String, StatsHolder> statsHolderPerPipeline = Collections.emptyMap();

    public PipelineExecutionService(PipelineStore store, ThreadPool threadPool) {
        this.store = store;
        this.threadPool = threadPool;
    }

    public void executeIndexRequest(IndexRequest request, Consumer<Exception> failureHandler, Consumer<Boolean> completionHandler) {
        // we don't need this anymore as index api is a sugar api
    }

    public void executeBulkRequest(Iterable<DocWriteRequest> actionRequests,
                                   BiConsumer<IndexRequest, Exception> itemFailureHandler,
                                   Consumer<Exception> completionHandler) {
        Stream<Tuple<IndexRequest, String>> stream = StreamSupport.stream(actionRequests.spliterator(), false)
            .filter(r -> r instanceof IndexRequest)
            .map(r -> (IndexRequest) r)
            .filter(indexRequest -> Strings.hasText(indexRequest.getPipeline()))
            .map(indexRequest -> new Tuple<>(indexRequest, indexRequest.getPipeline()));

        Function<IndexRequest, IngestDocument> ingestDocumentFactory = indexRequest -> {
            String index = indexRequest.index();
            String type = indexRequest.type();
            String id = indexRequest.id();
            String routing = indexRequest.routing();
            String parent = indexRequest.parent();
            Map<String, Object> sourceAsMap = indexRequest.sourceAsMap();
            return new IngestDocument(index, type, id, routing, parent, sourceAsMap);
        };

        BiConsumer<IndexRequest, IngestDocument> postProcessor = (indexRequest, ingestDocument) -> {
            Map<IngestDocument.MetaData, String> metadataMap = ingestDocument.extractMetadata();
            //it's fine to set all metadata fields all the time, as ingest document holds their starting values
            //before ingestion, which might also get modified during ingestion.
            indexRequest.index(metadataMap.get(IngestDocument.MetaData.INDEX));
            indexRequest.type(metadataMap.get(IngestDocument.MetaData.TYPE));
            indexRequest.id(metadataMap.get(IngestDocument.MetaData.ID));
            indexRequest.routing(metadataMap.get(IngestDocument.MetaData.ROUTING));
            indexRequest.parent(metadataMap.get(IngestDocument.MetaData.PARENT));
            indexRequest.source(ingestDocument.getSourceAndMetadata());
        };
        executeRequest(ThreadPool.Names.BULK, stream, ingestDocumentFactory, postProcessor, itemFailureHandler, completionHandler);
    }

    public <R> void executeRequest(String threadPoolName, Stream<Tuple<R, String>> requests,
                                   Function<R, IngestDocument> ingestDocumentFactory, BiConsumer<R, IngestDocument> postProcessor,
                                   BiConsumer<R, Exception> itemFailureHandler, Consumer<Exception> completionHandler) {
        threadPool.executor(threadPoolName).execute(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                completionHandler.accept(e);
            }

            @Override
            protected void doRun() throws Exception {
                Iterator<Tuple<R, String>> iterator = requests.iterator();
                while (iterator.hasNext()){
                    Tuple<R, String> tuple = iterator.next();
                    R request = tuple.v1();
                    Pipeline pipeline = getPipeline(tuple.v2());
                    try {
                        innerExecute(request, pipeline, ingestDocumentFactory, postProcessor);
                    } catch (Exception e) {
                        itemFailureHandler.accept(request, e);
                    }
                }
                completionHandler.accept(null);
            }
        });
    }

    public IngestStats stats() {
        Map<String, StatsHolder> statsHolderPerPipeline = this.statsHolderPerPipeline;

        Map<String, IngestStats.Stats> statsPerPipeline = new HashMap<>(statsHolderPerPipeline.size());
        for (Map.Entry<String, StatsHolder> entry : statsHolderPerPipeline.entrySet()) {
            statsPerPipeline.put(entry.getKey(), entry.getValue().createStats());
        }

        return new IngestStats(totalStats.createStats(), statsPerPipeline);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        IngestMetadata ingestMetadata = event.state().getMetaData().custom(IngestMetadata.TYPE);
        if (ingestMetadata != null) {
            updatePipelineStats(ingestMetadata);
        }
    }

    void updatePipelineStats(IngestMetadata ingestMetadata) {
        boolean changed = false;
        Map<String, StatsHolder> newStatsPerPipeline = new HashMap<>(statsHolderPerPipeline);
        Iterator<String> iterator = newStatsPerPipeline.keySet().iterator();
        while (iterator.hasNext()) {
            String pipeline = iterator.next();
            if (ingestMetadata.getPipelines().containsKey(pipeline) == false) {
                iterator.remove();
                changed = true;
            }
        }
        for (String pipeline : ingestMetadata.getPipelines().keySet()) {
            if (newStatsPerPipeline.containsKey(pipeline) == false) {
                newStatsPerPipeline.put(pipeline, new StatsHolder());
                changed = true;
            }
        }

        if (changed) {
            statsHolderPerPipeline = Collections.unmodifiableMap(newStatsPerPipeline);
        }
    }

    private <R> void innerExecute(R request, Pipeline pipeline, Function<R, IngestDocument> ingestDocumentFactory,
                                  BiConsumer<R, IngestDocument> postProcessor) throws Exception {
        if (pipeline.getProcessors().isEmpty()) {
            return;
        }

        long startTimeInNanos = System.nanoTime();
        // the pipeline specific stat holder may not exist and that is fine:
        // (e.g. the pipeline may have been removed while we're ingesting a document
        Optional<StatsHolder> pipelineStats = Optional.ofNullable(statsHolderPerPipeline.get(pipeline.getId()));
        try {
            totalStats.preIngest();
            pipelineStats.ifPresent(StatsHolder::preIngest);
            IngestDocument ingestDocument = ingestDocumentFactory.apply(request);
            pipeline.execute(ingestDocument);
            postProcessor.accept(request, ingestDocument);
        } catch (Exception e) {
            totalStats.ingestFailed();
            pipelineStats.ifPresent(StatsHolder::ingestFailed);
            throw e;
        } finally {
            long ingestTimeInMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeInNanos);
            totalStats.postIngest(ingestTimeInMillis);
            pipelineStats.ifPresent(statsHolder -> statsHolder.postIngest(ingestTimeInMillis));
        }
    }

    private Pipeline getPipeline(String pipelineId) {
        Pipeline pipeline = store.get(pipelineId);
        if (pipeline == null) {
            throw new IllegalArgumentException("pipeline with id [" + pipelineId + "] does not exist");
        }
        return pipeline;
    }

    static class StatsHolder {

        private final MeanMetric ingestMetric = new MeanMetric();
        private final CounterMetric ingestCurrent = new CounterMetric();
        private final CounterMetric ingestFailed = new CounterMetric();

        void preIngest() {
            ingestCurrent.inc();
        }

        void postIngest(long ingestTimeInMillis) {
            ingestCurrent.dec();
            ingestMetric.inc(ingestTimeInMillis);
        }

        void ingestFailed() {
            ingestFailed.inc();
        }

        IngestStats.Stats createStats() {
            return new IngestStats.Stats(ingestMetric.count(), ingestMetric.sum(), ingestCurrent.count(), ingestFailed.count());
        }

    }

    public interface Handler {

        void handle() throws Exception;

    }

}
