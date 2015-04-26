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

package org.elasticsearch.index.shard;

import com.google.common.base.Charsets;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilteredQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.BitDocIdSetFilter;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.ThreadInterruptedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.action.WriteFailureException;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.aliases.IndexAliasesService;
import org.elasticsearch.index.cache.IndexCache;
import org.elasticsearch.index.cache.bitset.ShardBitsetFilterCache;
import org.elasticsearch.index.cache.filter.FilterCacheStats;
import org.elasticsearch.index.cache.filter.ShardFilterCache;
import org.elasticsearch.index.cache.id.IdCacheStats;
import org.elasticsearch.index.cache.query.ShardQueryCache;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.deletionpolicy.SnapshotDeletionPolicy;
import org.elasticsearch.index.deletionpolicy.SnapshotIndexCommit;
import org.elasticsearch.index.engine.*;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.fielddata.IndexFieldDataService;
import org.elasticsearch.index.fielddata.ShardFieldData;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.get.ShardGetService;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.indexing.ShardIndexingService;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.policy.MergePolicyProvider;
import org.elasticsearch.index.merge.scheduler.MergeSchedulerProvider;
import org.elasticsearch.index.percolator.PercolatorQueriesRegistry;
import org.elasticsearch.index.percolator.stats.ShardPercolateService;
import org.elasticsearch.index.query.IndexQueryParserService;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.nested.NonNestedDocsFilter;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.search.stats.ShardSearchService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.similarity.SimilarityService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.Store.MetadataSnapshot;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.suggest.stats.ShardSuggestService;
import org.elasticsearch.index.suggest.stats.SuggestStats;
import org.elasticsearch.index.termvectors.ShardTermVectorsService;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.warmer.ShardIndexWarmerService;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.indices.IndicesLifecycle;
import org.elasticsearch.indices.IndicesWarmer;
import org.elasticsearch.indices.InternalIndicesLifecycle;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.search.suggest.completion.Completion090PostingsFormat;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.channels.ClosedByInterruptException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.mapper.SourceToParse.source;

/**
 *
 */
public class IndexShard extends AbstractIndexShardComponent {

    private final ThreadPool threadPool;
    private final IndexSettingsService indexSettingsService;
    private final MapperService mapperService;
    private final IndexQueryParserService queryParserService;
    private final IndexCache indexCache;
    private final InternalIndicesLifecycle indicesLifecycle;
    private final Store store;
    private final MergeSchedulerProvider mergeScheduler;
    private final Translog translog;
    private final IndexAliasesService indexAliasesService;
    private final ShardIndexingService indexingService;
    private final ShardSearchService searchService;
    private final ShardGetService getService;
    private final ShardIndexWarmerService shardWarmerService;
    private final ShardFilterCache shardFilterCache;
    private final ShardQueryCache shardQueryCache;
    private final ShardFieldData shardFieldData;
    private final PercolatorQueriesRegistry percolatorQueriesRegistry;
    private final ShardPercolateService shardPercolateService;
    private final ShardTermVectorsService termVectorsService;
    private final IndexFieldDataService indexFieldDataService;
    private final IndexService indexService;
    private final ShardSuggestService shardSuggestService;
    private final ShardBitsetFilterCache shardBitsetFilterCache;
    private final DiscoveryNode localNode;

    private final Object mutex = new Object();
    private final String checkIndexOnStartup;
    private final NodeEnvironment nodeEnv;

    private TimeValue refreshInterval;

    private volatile ScheduledFuture refreshScheduledFuture;
    private volatile ScheduledFuture mergeScheduleFuture;
    protected volatile ShardRouting shardRouting;
    protected volatile IndexShardState state;
    protected final AtomicReference<Engine> currentEngineReference = new AtomicReference<>();
    protected final EngineConfig config;
    protected final EngineFactory engineFactory;

    @Nullable
    private RecoveryState recoveryState;

    private ApplyRefreshSettings applyRefreshSettings = new ApplyRefreshSettings();

    private final MeanMetric refreshMetric = new MeanMetric();
    private final MeanMetric flushMetric = new MeanMetric();

    private final ShardEngineFailListener failedEngineListener = new ShardEngineFailListener();

    private final MapperAnalyzer mapperAnalyzer;
    private volatile boolean flushOnClose = true;

    /**
     * Index setting to control if a flush is executed before engine is closed
     * This setting is realtime updateable.
     */
    public static final String INDEX_FLUSH_ON_CLOSE = "index.flush_on_close";

    @Inject
    public IndexShard(ShardId shardId, @IndexSettings Settings indexSettings, IndexSettingsService indexSettingsService, IndicesLifecycle indicesLifecycle, Store store, MergeSchedulerProvider mergeScheduler, Translog translog,
                      ThreadPool threadPool, MapperService mapperService, IndexQueryParserService queryParserService, IndexCache indexCache, IndexAliasesService indexAliasesService, ShardIndexingService indexingService, ShardGetService getService, ShardSearchService searchService, ShardIndexWarmerService shardWarmerService,
                      ShardFilterCache shardFilterCache, ShardFieldData shardFieldData, PercolatorQueriesRegistry percolatorQueriesRegistry, ShardPercolateService shardPercolateService, CodecService codecService,
                      ShardTermVectorsService termVectorsService, IndexFieldDataService indexFieldDataService, IndexService indexService, ShardSuggestService shardSuggestService, ShardQueryCache shardQueryCache, ShardBitsetFilterCache shardBitsetFilterCache,
                      @Nullable IndicesWarmer warmer, SnapshotDeletionPolicy deletionPolicy, SimilarityService similarityService, MergePolicyProvider mergePolicyProvider, EngineFactory factory,
                      ClusterService clusterService, NodeEnvironment nodeEnv) {
        super(shardId, indexSettings);
        Preconditions.checkNotNull(store, "Store must be provided to the index shard");
        Preconditions.checkNotNull(deletionPolicy, "Snapshot deletion policy must be provided to the index shard");
        Preconditions.checkNotNull(translog, "Translog must be provided to the index shard");
        this.engineFactory = factory;
        this.indicesLifecycle = (InternalIndicesLifecycle) indicesLifecycle;
        this.indexSettingsService = indexSettingsService;
        this.store = store;
        this.mergeScheduler = mergeScheduler;
        this.translog = translog;
        this.threadPool = threadPool;
        this.mapperService = mapperService;
        this.queryParserService = queryParserService;
        this.indexCache = indexCache;
        this.indexAliasesService = indexAliasesService;
        this.indexingService = indexingService;
        this.getService = getService.setIndexShard(this);
        this.termVectorsService = termVectorsService.setIndexShard(this);
        this.searchService = searchService;
        this.shardWarmerService = shardWarmerService;
        this.shardFilterCache = shardFilterCache;
        this.shardQueryCache = shardQueryCache;
        this.shardFieldData = shardFieldData;
        this.percolatorQueriesRegistry = percolatorQueriesRegistry;
        this.shardPercolateService = shardPercolateService;
        this.indexFieldDataService = indexFieldDataService;
        this.indexService = indexService;
        this.shardSuggestService = shardSuggestService;
        this.shardBitsetFilterCache = shardBitsetFilterCache;
        assert clusterService.localNode() != null : "Local node is null lifecycle state is: " + clusterService.lifecycleState();
        this.localNode = clusterService.localNode();
        state = IndexShardState.CREATED;
        this.refreshInterval = indexSettings.getAsTime(INDEX_REFRESH_INTERVAL, EngineConfig.DEFAULT_REFRESH_INTERVAL);
        this.flushOnClose = indexSettings.getAsBoolean(INDEX_FLUSH_ON_CLOSE, true);
        this.nodeEnv = nodeEnv;
        indexSettingsService.addListener(applyRefreshSettings);

        this.mapperAnalyzer = new MapperAnalyzer(mapperService);
        /* create engine config */

        this.config = new EngineConfig(shardId,
                indexSettings.getAsBoolean(EngineConfig.INDEX_OPTIMIZE_AUTOGENERATED_ID_SETTING, false),
                threadPool,indexingService,indexSettingsService, warmer, store, deletionPolicy, translog, mergePolicyProvider, mergeScheduler,
                mapperAnalyzer, similarityService.similarity(), codecService, failedEngineListener);

        logger.debug("state: [CREATED]");

        this.checkIndexOnStartup = indexSettings.get("index.shard.check_on_startup", "false");
    }

    public Store store() {
        return this.store;
    }

    public Translog translog() {
        return translog;
    }

    public ShardIndexingService indexingService() {
        return this.indexingService;
    }

    public ShardGetService getService() {
        return this.getService;
    }

    public ShardTermVectorsService termVectorsService() {
        return termVectorsService;
    }

    public ShardSuggestService shardSuggestService() {
        return shardSuggestService;
    }

    public ShardBitsetFilterCache shardBitsetFilterCache() {
        return shardBitsetFilterCache;
    }

    public IndexFieldDataService indexFieldDataService() {
        return indexFieldDataService;
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public IndexService indexService() {
        return indexService;
    }

    public ShardSearchService searchService() {
        return this.searchService;
    }

    public ShardIndexWarmerService warmerService() {
        return this.shardWarmerService;
    }

    public ShardFilterCache filterCache() {
        return this.shardFilterCache;
    }

    public ShardQueryCache queryCache() {
        return this.shardQueryCache;
    }

    public ShardFieldData fieldData() {
        return this.shardFieldData;
    }

    /**
     * Returns the latest cluster routing entry received with this shard. Might be null if the
     * shard was just created.
     */
    public ShardRouting routingEntry() {
        return this.shardRouting;
    }

    /**
     * Updates the shards routing entry. This mutate the shards internal state depending
     * on the changes that get introduced by the new routing value. This method will persist shard level metadata
     * unless explicitly disabled.
     */
    public void updateRoutingEntry(final ShardRouting newRouting, final boolean persistState) {
        final ShardRouting currentRouting = this.shardRouting;
        if (!newRouting.shardId().equals(shardId())) {
            throw new ElasticsearchIllegalArgumentException("Trying to set a routing entry with shardId [" + newRouting.shardId() + "] on a shard with shardId [" + shardId() + "]");
        }
        try {
            if (currentRouting != null) {
                assert newRouting.version() > currentRouting.version() : "expected: " + newRouting.version() + " > " + currentRouting.version();
                if (!newRouting.primary() && currentRouting.primary()) {
                    logger.warn("suspect illegal state: trying to move shard from primary mode to replica mode");
                }
                // if its the same routing, return
                if (currentRouting.equals(newRouting)) {
                    this.shardRouting = newRouting; // might have a new version
                    return;
                }
            }

            if (state == IndexShardState.POST_RECOVERY) {
                // if the state is started or relocating (cause it might move right away from started to relocating)
                // then move to STARTED
                if (newRouting.state() == ShardRoutingState.STARTED || newRouting.state() == ShardRoutingState.RELOCATING) {
                    // we want to refresh *before* we move to internal STARTED state
                    try {
                        engine().refresh("cluster_state_started");
                    } catch (Throwable t) {
                        logger.debug("failed to refresh due to move to cluster wide started", t);
                    }

                    boolean movedToStarted = false;
                    synchronized (mutex) {
                        // do the check under a mutex, so we make sure to only change to STARTED if in POST_RECOVERY
                        if (state == IndexShardState.POST_RECOVERY) {
                            changeState(IndexShardState.STARTED, "global state is [" + newRouting.state() + "]");
                            movedToStarted = true;
                        } else {
                            logger.debug("state [{}] not changed, not in POST_RECOVERY, global state is [{}]", state, newRouting.state());
                        }
                    }
                    if (movedToStarted) {
                        indicesLifecycle.afterIndexShardStarted(this);
                    }
                }
            }
            this.shardRouting = newRouting;
            indicesLifecycle.shardRoutingChanged(this, currentRouting, newRouting);
        } finally {
            if (persistState) {
                persistMetadata(newRouting, currentRouting);
            }
        }
    }

    /**
     * Marks the shard as recovering based on a remote or local node, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState recovering(String reason, RecoveryState.Type type, DiscoveryNode sourceNode) throws IndexShardStartedException,
            IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        return recovering(reason, new RecoveryState(shardId, shardRouting.primary(), type, sourceNode, localNode));
    }

    /**
     * Marks the shard as recovering based on a restore, fails with exception is recovering is not allowed to be set.
     */
    public IndexShardState recovering(String reason, RecoveryState.Type type, RestoreSource restoreSource) throws IndexShardStartedException {
        return recovering(reason, new RecoveryState(shardId, shardRouting.primary(), type, restoreSource, localNode));
    }

    private IndexShardState recovering(String reason, RecoveryState recoveryState) throws IndexShardStartedException,
            IndexShardRelocatedException, IndexShardRecoveringException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            if (state == IndexShardState.RECOVERING) {
                throw new IndexShardRecoveringException(shardId);
            }
            if (state == IndexShardState.POST_RECOVERY) {
                throw new IndexShardRecoveringException(shardId);
            }
            this.recoveryState = recoveryState;
            return changeState(IndexShardState.RECOVERING, reason);
        }
    }

    public IndexShard relocated(String reason) throws IndexShardNotStartedException {
        synchronized (mutex) {
            if (state != IndexShardState.STARTED) {
                throw new IndexShardNotStartedException(shardId, state);
            }
            changeState(IndexShardState.RELOCATED, reason);
        }
        return this;
    }

    public IndexShardState state() {
        return state;
    }

    /**
     * Changes the state of the current shard
     *
     * @param newState the new shard state
     * @param reason   the reason for the state change
     * @return the previous shard state
     */
    private IndexShardState changeState(IndexShardState newState, String reason) {
        logger.debug("state: [{}]->[{}], reason [{}]", state, newState, reason);
        IndexShardState previousState = state;
        state = newState;
        this.indicesLifecycle.indexShardStateChanged(this, previousState, reason);
        return previousState;
    }

    public Engine.Create prepareCreate(SourceToParse source, long version, VersionType versionType, Engine.Operation.Origin origin, boolean canHaveDuplicates, boolean autoGeneratedId) throws ElasticsearchException {
        long startTime = System.nanoTime();
        Tuple<DocumentMapper, Boolean> docMapper = mapperService.documentMapperWithAutoCreate(source.type());
        try {
            ParsedDocument doc = docMapper.v1().parse(source).setMappingsModified(docMapper);
            return new Engine.Create(docMapper.v1(), docMapper.v1().uidMapper().term(doc.uid().stringValue()), doc, version, versionType, origin, startTime, state != IndexShardState.STARTED || canHaveDuplicates, autoGeneratedId);
        } catch (Throwable t) {
            if (docMapper.v2()) {
                throw new WriteFailureException(t, docMapper.v1().type());
            } else {
                throw t;
            }
        }
    }

    public ParsedDocument create(Engine.Create create) throws ElasticsearchException {
        writeAllowed(create.origin());
        create = indexingService.preCreate(create);
        mapperAnalyzer.setType(create.type());
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}][{}]{}", create.type(), create.id(), create.docs());
            }
            engine().create(create);
            create.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingService.postCreate(create, ex);
            throw ex;
        }
        indexingService.postCreate(create);
        return create.parsedDoc();
    }

    public Engine.Index prepareIndex(SourceToParse source, long version, VersionType versionType, Engine.Operation.Origin origin, boolean canHaveDuplicates) throws ElasticsearchException {
        long startTime = System.nanoTime();
        Tuple<DocumentMapper, Boolean> docMapper = mapperService.documentMapperWithAutoCreate(source.type());
        try {
            ParsedDocument doc = docMapper.v1().parse(source).setMappingsModified(docMapper);
            return new Engine.Index(docMapper.v1(), docMapper.v1().uidMapper().term(doc.uid().stringValue()), doc, version, versionType, origin, startTime, state != IndexShardState.STARTED || canHaveDuplicates);
        } catch (Throwable t) {
            if (docMapper.v2()) {
                throw new WriteFailureException(t, docMapper.v1().type());
            } else {
                throw t;
            }
        }
    }

    public ParsedDocument index(Engine.Index index) throws ElasticsearchException {
        writeAllowed(index.origin());
        index = indexingService.preIndex(index);
        mapperAnalyzer.setType(index.type());
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}][{}]{}", index.type(), index.id(), index.docs());
            }
            engine().index(index);
            index.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingService.postIndex(index, ex);
            throw ex;
        }
        indexingService.postIndex(index);
        return index.parsedDoc();
    }

    public Engine.Delete prepareDelete(String type, String id, long version, VersionType versionType, Engine.Operation.Origin origin) throws ElasticsearchException {
        long startTime = System.nanoTime();
        DocumentMapper docMapper = mapperService.documentMapperWithAutoCreate(type).v1();
        return new Engine.Delete(type, id, docMapper.uidMapper().term(type, id), version, versionType, origin, startTime, false);
    }

    public void delete(Engine.Delete delete) throws ElasticsearchException {
        writeAllowed(delete.origin());
        delete = indexingService.preDelete(delete);
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("delete [{}]", delete.uid().text());
            }
            engine().delete(delete);
            delete.endTime(System.nanoTime());
        } catch (Throwable ex) {
            indexingService.postDelete(delete, ex);
            throw ex;
        }
        indexingService.postDelete(delete);
    }

    public Engine.DeleteByQuery prepareDeleteByQuery(BytesReference source, @Nullable String[] filteringAliases, Engine.Operation.Origin origin, String... types) throws ElasticsearchException {
        long startTime = System.nanoTime();
        if (types == null) {
            types = Strings.EMPTY_ARRAY;
        }
        Query query = queryParserService.parseQuery(source).query();
        query = filterQueryIfNeeded(query, types);

        Filter aliasFilter = indexAliasesService.aliasFilter(filteringAliases);
        BitDocIdSetFilter parentFilter = mapperService.hasNested() ? indexCache.bitsetFilterCache().getBitDocIdSetFilter(NonNestedDocsFilter.INSTANCE) : null;
        return new Engine.DeleteByQuery(query, source, filteringAliases, aliasFilter, parentFilter, origin, startTime, types);
    }

    public void deleteByQuery(Engine.DeleteByQuery deleteByQuery) throws ElasticsearchException {
        writeAllowed(deleteByQuery.origin());
        if (logger.isTraceEnabled()) {
            logger.trace("delete_by_query [{}]", deleteByQuery.query());
        }
        deleteByQuery = indexingService.preDeleteByQuery(deleteByQuery);
        engine().delete(deleteByQuery);
        deleteByQuery.endTime(System.nanoTime());
        indexingService.postDeleteByQuery(deleteByQuery);
    }

    public Engine.GetResult get(Engine.Get get) throws ElasticsearchException {
        readAllowed();
        Set<FieldMapper<?>> includeFields = indexService.aliasesService().aliasFields(get.indexOrAlias());
        if (includeFields.isEmpty() == false) {
            assert get.realtime() == false : "realtime option is incompatible with includeFields option";
            get.includeFields(includeFields.toArray(new FieldMapper[]{}));
        }
        return engine().get(get);
    }

    public void refresh(String source) throws ElasticsearchException {
        verifyNotClosed();
        if (logger.isTraceEnabled()) {
            logger.trace("refresh with soruce: {}", source);
        }
        long time = System.nanoTime();
        engine().refresh(source);
        refreshMetric.inc(System.nanoTime() - time);
    }

    public RefreshStats refreshStats() {
        return new RefreshStats(refreshMetric.count(), TimeUnit.NANOSECONDS.toMillis(refreshMetric.sum()));
    }

    public FlushStats flushStats() {
        return new FlushStats(flushMetric.count(), TimeUnit.NANOSECONDS.toMillis(flushMetric.sum()));
    }

    public DocsStats docStats() {
        final Engine.Searcher searcher = acquireSearcher("doc_stats");
        try {
            return new DocsStats(searcher.reader().numDocs(), searcher.reader().numDeletedDocs());
        } finally {
            searcher.close();
        }
    }

    public IndexingStats indexingStats(String... types) {
        return indexingService.stats(types);
    }

    public SearchStats searchStats(String... groups) {
        return searchService.stats(groups);
    }

    public GetStats getStats() {
        return getService.stats();
    }

    public StoreStats storeStats() {
        try {
            return store.stats();
        } catch (IOException e) {
            throw new ElasticsearchException("io exception while building 'store stats'", e);
        } catch (AlreadyClosedException ex) {
            return null; // already closed
        }
    }

    public MergeStats mergeStats() {
        return mergeScheduler.stats();
    }

    public SegmentsStats segmentStats() {
        SegmentsStats segmentsStats = engine().segmentsStats();
        segmentsStats.addBitsetMemoryInBytes(shardBitsetFilterCache.getMemorySizeInBytes());
        return segmentsStats;
    }

    public WarmerStats warmerStats() {
        return shardWarmerService.stats();
    }

    public FilterCacheStats filterCacheStats() {
        return shardFilterCache.stats();
    }

    public FieldDataStats fieldDataStats(String... fields) {
        return shardFieldData.stats(fields);
    }

    public PercolatorQueriesRegistry percolateRegistry() {
        return percolatorQueriesRegistry;
    }

    public ShardPercolateService shardPercolateService() {
        return shardPercolateService;
    }

    public IdCacheStats idCacheStats() {
        long memorySizeInBytes = shardFieldData.stats(ParentFieldMapper.NAME).getFields().get(ParentFieldMapper.NAME);
        return new IdCacheStats(memorySizeInBytes);
    }

    public TranslogStats translogStats() {
        return translog.stats();
    }

    public SuggestStats suggestStats() {
        return shardSuggestService.stats();
    }

    public CompletionStats completionStats(String... fields) {
        CompletionStats completionStats = new CompletionStats();
        final Engine.Searcher currentSearcher = acquireSearcher("completion_stats");
        try {
            PostingsFormat postingsFormat = PostingsFormat.forName(Completion090PostingsFormat.CODEC_NAME);
            if (postingsFormat instanceof Completion090PostingsFormat) {
                Completion090PostingsFormat completionPostingsFormat = (Completion090PostingsFormat) postingsFormat;
                completionStats.add(completionPostingsFormat.completionStats(currentSearcher.reader(), fields));
            }
        } finally {
            currentSearcher.close();
        }
        return completionStats;
    }

    public void flush(FlushRequest request) throws ElasticsearchException {
        // we allows flush while recovering, since we allow for operations to happen
        // while recovering, and we want to keep the translog at bay (up to deletes, which
        // we don't gc).
        verifyStartedOrRecovering();
        if (logger.isTraceEnabled()) {
            logger.trace("flush with {}", request);
        }
        long time = System.nanoTime();
        engine().flush(request.force(), request.waitIfOngoing());
        flushMetric.inc(System.nanoTime() - time);
    }

    public void optimize(OptimizeRequest optimize) throws ElasticsearchException {
        verifyStarted();
        if (logger.isTraceEnabled()) {
            logger.trace("optimize with {}", optimize);
        }
        engine().forceMerge(optimize.flush(), optimize.maxNumSegments(), optimize.onlyExpungeDeletes(), optimize.upgrade());
    }

    public SnapshotIndexCommit snapshotIndex() throws EngineException {
        IndexShardState state = this.state; // one time volatile read
        // we allow snapshot on closed index shard, since we want to do one after we close the shard and before we close the engine
        if (state == IndexShardState.STARTED || state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED) {
            return engine().snapshotIndex();
        } else {
            throw new IllegalIndexShardStateException(shardId, state, "snapshot is not allowed");
        }
    }

    public void recover(Engine.RecoveryHandler recoveryHandler) throws EngineException {
        verifyStarted();
        engine().recover(recoveryHandler);
    }

    public void failShard(String reason, Throwable e) {
        // fail the engine. This will cause this shard to also be removed from the node's index service.
        engine().failEngine(reason, e);
    }

    public Engine.Searcher acquireSearcher(String source) {
        return acquireSearcher(source, false);
    }

    public Engine.Searcher acquireSearcher(String source, String... aliases) {
        if (aliases == null) {
            return acquireSearcher(source);
        }

        Set<FieldMapper<?>> includeFields = indexService.aliasesService().aliasFields(aliases);
        if (includeFields.isEmpty() == false) {
            return acquireSearcher(source, false, includeFields.toArray(new FieldMapper[0]));
        } else {
            return acquireSearcher(source);
        }
    }

    public Engine.Searcher acquireSearcher(String source, boolean searcherForWriteOperation, FieldMapper<?>... includeFields) {
        readAllowed(searcherForWriteOperation);
        return engine().acquireSearcher(source, includeFields);
    }

    public void close(String reason, boolean flushEngine) throws IOException {
        synchronized (mutex) {
            try {
                indexSettingsService.removeListener(applyRefreshSettings);
                if (state != IndexShardState.CLOSED) {
                    FutureUtils.cancel(refreshScheduledFuture);
                    refreshScheduledFuture = null;
                    FutureUtils.cancel(mergeScheduleFuture);
                    mergeScheduleFuture = null;
                }
                changeState(IndexShardState.CLOSED, reason);
            } finally {
                final Engine engine = this.currentEngineReference.getAndSet(null);
                try {
                    if (flushEngine && this.flushOnClose) {
                        engine.flushAndClose();
                    }
                } finally { // playing safe here and close the engine even if the above succeeds - close can be called multiple times
                    IOUtils.close(engine);
                }
            }
        }
    }

    public IndexShard postRecovery(String reason) throws IndexShardStartedException, IndexShardRelocatedException, IndexShardClosedException {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new IndexShardClosedException(shardId);
            }
            if (state == IndexShardState.STARTED) {
                throw new IndexShardStartedException(shardId);
            }
            if (state == IndexShardState.RELOCATED) {
                throw new IndexShardRelocatedException(shardId);
            }
            recoveryState.setStage(RecoveryState.Stage.DONE);
            changeState(IndexShardState.POST_RECOVERY, reason);
        }
        indicesLifecycle.afterIndexShardPostRecovery(this);
        return this;
    }

    /** called before starting to copy index files over */
    public void prepareForIndexRecovery() throws ElasticsearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.INDEX);
        assert currentEngineReference.get() == null;
    }

    /**
     * After the store has been recovered, we need to start the engine in order to apply operations
     */
    public void prepareForTranslogRecovery() throws ElasticsearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        recoveryState.setStage(RecoveryState.Stage.START);
        // also check here, before we apply the translog
        if (Booleans.parseBoolean(checkIndexOnStartup, false)) {
            checkIndex();
        }
        // we disable deletes since we allow for operations to be executed against the shard while recovering
        // but we need to make sure we don't loose deletes until we are done recovering
        config.setEnableGcDeletes(false);
        createNewEngine();
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
    }

    /** called if recovery has to be restarted after network error / delay ** */
    public void performRecoveryRestart() throws IOException {
        synchronized (mutex) {
            if (state != IndexShardState.RECOVERING) {
                throw new IndexShardNotRecoveringException(shardId, state);
            }
            final Engine engine = this.currentEngineReference.getAndSet(null);
            IOUtils.close(engine);
            recoveryState().setStage(RecoveryState.Stage.INIT);
        }
    }

    /**
     * Returns the current {@link RecoveryState} if this shard is recovering or has been recovering.
     * Returns null if the recovery has not yet started or shard was not recovered (created via an API).
     */
    public RecoveryState recoveryState() {
        return this.recoveryState;
    }

    /**
     * perform the last stages of recovery once all translog operations are done.
     * note that you should still call {@link #postRecovery(String)}.
     */
    public void finalizeRecovery() {
        recoveryState().setStage(RecoveryState.Stage.FINALIZE);
        // clear unreferenced files
        translog.clearUnreferenced();
        engine().refresh("recovery_finalization");
        startScheduledTasksIfNeeded();
        config.setEnableGcDeletes(true);
    }

    /**
     * Performs a single recovery operation, and returns the indexing operation (or null if its not an indexing operation)
     * that can then be used for mapping updates (for example) if needed.
     */
    public Engine.IndexingOperation performRecoveryOperation(Translog.Operation operation) throws ElasticsearchException {
        if (state != IndexShardState.RECOVERING) {
            throw new IndexShardNotRecoveringException(shardId, state);
        }
        Engine.IndexingOperation indexOperation = null;
        try {
            switch (operation.opType()) {
                case CREATE:
                    Translog.Create create = (Translog.Create) operation;
                    Engine.Create engineCreate = prepareCreate(
                            source(create.source()).type(create.type()).id(create.id())
                                    .routing(create.routing()).parent(create.parent()).timestamp(create.timestamp()).ttl(create.ttl()),
                            create.version(), create.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, true, false);
                    mapperAnalyzer.setType(create.type());
                    engine().create(engineCreate);
                    indexOperation = engineCreate;
                    break;
                case SAVE:
                    Translog.Index index = (Translog.Index) operation;
                    Engine.Index engineIndex = prepareIndex(source(index.source()).type(index.type()).id(index.id())
                                    .routing(index.routing()).parent(index.parent()).timestamp(index.timestamp()).ttl(index.ttl()),
                            index.version(), index.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, true);
                    mapperAnalyzer.setType(index.type());
                    engine().index(engineIndex);
                    indexOperation = engineIndex;
                    break;
                case DELETE:
                    Translog.Delete delete = (Translog.Delete) operation;
                    Uid uid = Uid.createUid(delete.uid().text());
                    engine().delete(new Engine.Delete(uid.type(), uid.id(), delete.uid(), delete.version(),
                            delete.versionType().versionTypeForReplicationAndRecovery(), Engine.Operation.Origin.RECOVERY, System.nanoTime(), false));
                    break;
                case DELETE_BY_QUERY:
                    Translog.DeleteByQuery deleteByQuery = (Translog.DeleteByQuery) operation;
                    engine().delete(prepareDeleteByQuery(deleteByQuery.source(), deleteByQuery.filteringAliases(), Engine.Operation.Origin.RECOVERY, deleteByQuery.types()));
                    break;
                default:
                    throw new ElasticsearchIllegalStateException("No operation defined for [" + operation + "]");
            }
        } catch (ElasticsearchException e) {
            boolean hasIgnoreOnRecoveryException = false;
            ElasticsearchException current = e;
            while (true) {
                if (current instanceof IgnoreOnRecoveryEngineException) {
                    hasIgnoreOnRecoveryException = true;
                    break;
                }
                if (current.getCause() instanceof ElasticsearchException) {
                    current = (ElasticsearchException) current.getCause();
                } else {
                    break;
                }
            }
            if (!hasIgnoreOnRecoveryException) {
                throw e;
            }
        }
        return indexOperation;
    }

    /**
     * Returns <tt>true</tt> if this shard can ignore a recovery attempt made to it (since the already doing/done it)
     */
    public boolean ignoreRecoveryAttempt() {
        IndexShardState state = state(); // one time volatile read
        return state == IndexShardState.POST_RECOVERY || state == IndexShardState.RECOVERING || state == IndexShardState.STARTED ||
                state == IndexShardState.RELOCATED || state == IndexShardState.CLOSED;
    }

    public void readAllowed() throws IllegalIndexShardStateException {
        readAllowed(false);
    }


    private void readAllowed(boolean writeOperation) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (writeOperation) {
            if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED && state != IndexShardState.RECOVERING && state != IndexShardState.POST_RECOVERY) {
                throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when started/relocated");
            }
        } else {
            if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
                throw new IllegalIndexShardStateException(shardId, state, "operations only allowed when started/relocated");
            }
        }
    }

    private void writeAllowed(Engine.Operation.Origin origin) throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read

        if (origin == Engine.Operation.Origin.PRIMARY) {
            // for primaries, we only allow to write when actually started (so the cluster has decided we started)
            // otherwise, we need to retry, we also want to still allow to index if we are relocated in case it fails
            if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering, origin [" + origin + "]");
            }
        } else {
            // for replicas, we allow to write also while recovering, since we index also during recovery to replicas
            // and rely on version checks to make sure its consistent
            if (state != IndexShardState.STARTED && state != IndexShardState.RELOCATED && state != IndexShardState.RECOVERING && state != IndexShardState.POST_RECOVERY) {
                throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering, origin [" + origin + "]");
            }
        }
    }

    protected final void verifyStartedOrRecovering() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED && state != IndexShardState.RECOVERING && state != IndexShardState.POST_RECOVERY) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when started/recovering");
        }
    }

    private void verifyNotClosed() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state == IndexShardState.CLOSED) {
            throw new IllegalIndexShardStateException(shardId, state, "operation only allowed when not closed");
        }
    }

    protected final void verifyStarted() throws IllegalIndexShardStateException {
        IndexShardState state = this.state; // one time volatile read
        if (state != IndexShardState.STARTED) {
            throw new IndexShardNotStartedException(shardId, state);
        }
    }

    private void startScheduledTasksIfNeeded() {
        if (refreshInterval.millis() > 0) {
            refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, new EngineRefresher());
            logger.debug("scheduling refresher every {}", refreshInterval);
        } else {
            logger.debug("scheduled refresher disabled");
        }
    }

    private Query filterQueryIfNeeded(Query query, String[] types) {
        Filter searchFilter = mapperService.searchFilter(types);
        if (searchFilter != null) {
            query = new FilteredQuery(query, indexCache.filter().cache(searchFilter, null, indexService.queryParserService().autoFilterCachePolicy()));
        }
        return query;
    }

    public static final String INDEX_REFRESH_INTERVAL = "index.refresh_interval";

    public void addFailedEngineListener(Engine.FailedEngineListener failedEngineListener) {
        this.failedEngineListener.delegates.add(failedEngineListener);
    }

    public void updateBufferSize(ByteSizeValue shardIndexingBufferSize, ByteSizeValue shardTranslogBufferSize) {
        ByteSizeValue preValue = config.getIndexingBufferSize();
        config.setIndexingBufferSize(shardIndexingBufferSize);
        // update engine if it is already started.
        if (preValue.bytes() != shardIndexingBufferSize.bytes() && engineUnsafe() != null) {
            // its inactive, make sure we do a refresh / full IW flush in this case, since the memory
            // changes only after a "data" change has happened to the writer
            // the index writer lazily allocates memory and a refresh will clean it all up.
            if (shardIndexingBufferSize == EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER && preValue != EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER) {
                logger.debug("updating index_buffer_size from [{}] to (inactive) [{}]", preValue, shardIndexingBufferSize);
                try {
                    refresh("update index buffer");
                } catch (Throwable e) {
                    logger.warn("failed to refresh after setting shard to inactive", e);
                }
            } else {
                logger.debug("updating index_buffer_size from [{}] to [{}]", preValue, shardIndexingBufferSize);
            }
        }
        translog().updateBuffer(shardTranslogBufferSize);
    }

    public void markAsInactive() {
        updateBufferSize(EngineConfig.INACTIVE_SHARD_INDEXING_BUFFER, Translog.INACTIVE_SHARD_TRANSLOG_BUFFER);
    }

    public final boolean isFlushOnClose() {
        return flushOnClose;
    }

    private class ApplyRefreshSettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            boolean change = false;
            synchronized (mutex) {
                if (state == IndexShardState.CLOSED) {
                    return;
                }
                final boolean flushOnClose = settings.getAsBoolean(INDEX_FLUSH_ON_CLOSE, IndexShard.this.flushOnClose);
                if (flushOnClose != IndexShard.this.flushOnClose) {
                    logger.info("updating {} from [{}] to [{}]", INDEX_FLUSH_ON_CLOSE, IndexShard.this.flushOnClose, flushOnClose);
                    IndexShard.this.flushOnClose = flushOnClose;
                }

                TimeValue refreshInterval = settings.getAsTime(INDEX_REFRESH_INTERVAL, IndexShard.this.refreshInterval);
                if (!refreshInterval.equals(IndexShard.this.refreshInterval)) {
                    logger.info("updating refresh_interval from [{}] to [{}]", IndexShard.this.refreshInterval, refreshInterval);
                    if (refreshScheduledFuture != null) {
                        // NOTE: we pass false here so we do NOT attempt Thread.interrupt if EngineRefresher.run is currently running.  This is
                        // very important, because doing so can cause files to suddenly be closed if they were doing IO when the interrupt
                        // hit.  See https://issues.apache.org/jira/browse/LUCENE-2239
                        FutureUtils.cancel(refreshScheduledFuture);
                        refreshScheduledFuture = null;
                    }
                    IndexShard.this.refreshInterval = refreshInterval;
                    if (refreshInterval.millis() > 0) {
                        refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, new EngineRefresher());
                    }
                }

                long gcDeletesInMillis = settings.getAsTime(EngineConfig.INDEX_GC_DELETES_SETTING, TimeValue.timeValueMillis(config.getGcDeletesInMillis())).millis();
                if (gcDeletesInMillis != config.getGcDeletesInMillis()) {
                    logger.info("updating {} from [{}] to [{}]", EngineConfig.INDEX_GC_DELETES_SETTING, TimeValue.timeValueMillis(config.getGcDeletesInMillis()), TimeValue.timeValueMillis(gcDeletesInMillis));
                    config.setGcDeletesInMillis(gcDeletesInMillis);
                    change = true;
                }

                final boolean compoundOnFlush = settings.getAsBoolean(EngineConfig.INDEX_COMPOUND_ON_FLUSH, config.isCompoundOnFlush());
                if (compoundOnFlush != config.isCompoundOnFlush()) {
                    logger.info("updating {} from [{}] to [{}]", EngineConfig.INDEX_COMPOUND_ON_FLUSH, config.isCompoundOnFlush(), compoundOnFlush);
                    config.setCompoundOnFlush(compoundOnFlush);
                    change = true;
                }
                final String versionMapSize = settings.get(EngineConfig.INDEX_VERSION_MAP_SIZE, config.getVersionMapSizeSetting());
                if (config.getVersionMapSizeSetting().equals(versionMapSize) == false) {
                    config.setVersionMapSizeSetting(versionMapSize);
                }
            }
            if (change) {
                refresh("apply settings");
            }
        }
    }

    class EngineRefresher implements Runnable {
        @Override
        public void run() {
            // we check before if a refresh is needed, if not, we reschedule, otherwise, we fork, refresh, and then reschedule
            if (!engine().refreshNeeded()) {
                reschedule();
                return;
            }
            threadPool.executor(ThreadPool.Names.REFRESH).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (engine().refreshNeeded()) {
                            refresh("schedule");
                        }
                    } catch (EngineClosedException e) {
                        // we are being closed, ignore
                    } catch (RefreshFailedEngineException e) {
                        if (e.getCause() instanceof InterruptedException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof ClosedByInterruptException) {
                            // ignore, we are being shutdown
                        } else if (e.getCause() instanceof ThreadInterruptedException) {
                            // ignore, we are being shutdown
                        } else {
                            if (state != IndexShardState.CLOSED) {
                                logger.warn("Failed to perform scheduled engine refresh", e);
                            }
                        }
                    } catch (Exception e) {
                        if (state != IndexShardState.CLOSED) {
                            logger.warn("Failed to perform scheduled engine refresh", e);
                        }
                    }

                    reschedule();
                }
            });
        }

        /** Schedules another (future) refresh, if refresh_interval is still enabled. */
        private void reschedule() {
            synchronized (mutex) {
                if (state != IndexShardState.CLOSED && refreshInterval.millis() > 0) {
                    refreshScheduledFuture = threadPool.schedule(refreshInterval, ThreadPool.Names.SAME, this);
                }
            }
        }
    }
    
    private void checkIndex() throws IndexShardException {
        if (store.tryIncRef()) {
            try {
                doCheckIndex();
            } catch (IOException e) {
                throw new IndexShardException(shardId, "exception during checkindex", e);
            } finally {
                store.decRef();
            }
        }
    }

    private void doCheckIndex() throws IndexShardException, IOException {
        long time = System.currentTimeMillis();
        if (!Lucene.indexExists(store.directory())) {
            return;
        }
        BytesStreamOutput os = new BytesStreamOutput();
        PrintStream out = new PrintStream(os, false, Charsets.UTF_8.name());
        
        if ("checksum".equalsIgnoreCase(checkIndexOnStartup)) {
            // physical verification only: verify all checksums for the latest commit
            boolean corrupt = false;
            MetadataSnapshot metadata = store.getMetadata();
            for (Map.Entry<String,StoreFileMetaData> entry : metadata.asMap().entrySet()) {
                try {
                    Store.checkIntegrity(entry.getValue(), store.directory());
                    out.println("checksum passed: " + entry.getKey());
                } catch (IOException exc) {
                    out.println("checksum failed: " + entry.getKey());
                    exc.printStackTrace(out);
                    corrupt = true;
                }
            }
            out.flush();
            if (corrupt) {
                logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                throw new IndexShardException(shardId, "index check failure");
            }
        } else {
            // full checkindex
            try (CheckIndex checkIndex = new CheckIndex(store.directory())) {
                checkIndex.setInfoStream(out);
                CheckIndex.Status status = checkIndex.checkIndex();
                out.flush();
                
                if (!status.clean) {
                    if (state == IndexShardState.CLOSED) {
                        // ignore if closed....
                        return;
                    }
                    logger.warn("check index [failure]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
                    if ("fix".equalsIgnoreCase(checkIndexOnStartup)) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("fixing index, writing new segments file ...");
                        }
                        checkIndex.exorciseIndex(status);
                        if (logger.isDebugEnabled()) {
                            logger.debug("index fixed, wrote new segments file \"{}\"", status.segmentsFileName);
                        }
                    } else {
                        // only throw a failure if we are not going to fix the index
                        throw new IndexShardException(shardId, "index check failure");
                    }
                }
            }
        }
        
        if (logger.isDebugEnabled()) {
            logger.debug("check index [success]\n{}", new String(os.bytes().toBytes(), Charsets.UTF_8));
        }

        recoveryState.getStart().checkIndexTime(Math.max(0, System.currentTimeMillis() - time));
    }

    public Engine engine() {
        Engine engine = engineUnsafe();
        if (engine == null) {
            throw new EngineClosedException(shardId);
        }
        return engine;
    }

    protected Engine engineUnsafe() {
        return this.currentEngineReference.get();
    }

    class ShardEngineFailListener implements Engine.FailedEngineListener {
        private final CopyOnWriteArrayList<Engine.FailedEngineListener> delegates = new CopyOnWriteArrayList<>();

        // called by the current engine
        @Override
        public void onFailedEngine(ShardId shardId, String reason, @Nullable Throwable failure) {
            for (Engine.FailedEngineListener listener : delegates) {
                try {
                    listener.onFailedEngine(shardId, reason, failure);
                } catch (Exception e) {
                    logger.warn("exception while notifying engine failure", e);
                }
            }
        }
    }

    private void createNewEngine() {
        synchronized (mutex) {
            if (state == IndexShardState.CLOSED) {
                throw new EngineClosedException(shardId);
            }
            assert this.currentEngineReference.get() == null;
            this.currentEngineReference.set(newEngine());
        }
    }

    protected Engine newEngine() {
        return engineFactory.newReadWriteEngine(config);
    }


    /**
     * Returns <code>true</code> iff this shard allows primary promotion, otherwise <code>false</code>
     */
    public boolean allowsPrimaryPromotion() {
        return true;
    }

    // pkg private for testing
    void persistMetadata(ShardRouting newRouting, ShardRouting currentRouting) {
        assert newRouting != null : "newRouting must not be null";
        if (newRouting.active()) {
            try {
                final String writeReason;
                if (currentRouting == null) {
                    writeReason = "freshly started, version [" + newRouting.version() + "]";
                } else if (currentRouting.version() < newRouting.version()) {
                    writeReason = "version changed from [" + currentRouting.version() + "] to [" + newRouting.version() + "]";
                } else if (currentRouting.equals(newRouting) == false) {
                    writeReason = "routing changed from " + currentRouting + " to " + newRouting;
                } else {
                    logger.trace("skip writing shard state, has been written before; previous version:  [" +
                            currentRouting.version() + "] current version [" + newRouting.version() + "]");
                    assert currentRouting.version() <= newRouting.version() : "version should not go backwards for shardID: " + shardId +
                            " previous version:  [" + currentRouting.version() + "] current version [" + newRouting.version() + "]";
                    return;
                }
                final ShardStateMetaData newShardStateMetadata = new ShardStateMetaData(newRouting.version(), newRouting.primary(), getIndexUUID());
                ShardStateMetaData.write(logger, writeReason, shardId, newShardStateMetadata, currentRouting != null, nodeEnv.shardPaths(shardId));
            } catch (IOException e) { // this is how we used to handle it.... :(
                logger.warn("failed to write shard state", e);
                // we failed to write the shard state, we will try and write
                // it next time...
            }
        }
    }


    private String getIndexUUID() {
        assert indexSettings.get(IndexMetaData.SETTING_UUID) != null
                || indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT).before(Version.V_0_90_6):
        "version: " + indexSettings.getAsVersion(IndexMetaData.SETTING_VERSION_CREATED, null) + " uuid: " + indexSettings.get(IndexMetaData.SETTING_UUID) ;
        return indexSettings.get(IndexMetaData.SETTING_UUID, IndexMetaData.INDEX_UUID_NA_VALUE);
    }
}
