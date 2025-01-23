package org.tikv.raw;

import static org.tikv.common.util.ClientUtils.*;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;
import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.codec.KeyUtils;
import org.tikv.common.exception.*;
import org.tikv.common.importer.ImporterClient;
import org.tikv.common.importer.SwitchTiKVModeClient;
import org.tikv.common.key.Key;
import org.tikv.common.log.SlowLog;
import org.tikv.common.log.SlowLogEmptyImpl;
import org.tikv.common.log.SlowLogImpl;
import org.tikv.common.log.SlowLogSpan;
import org.tikv.common.operation.iterator.RawScanIteratorV2;
import org.tikv.common.operation.iterator.ScanIterator;
import org.tikv.common.region.*;
import org.tikv.common.store.StoreClient;
import org.tikv.common.store.StoreClientManager;
import org.tikv.common.util.*;
import org.tikv.kvproto.Kvrpcpb;

/** Raw TiKV client */
public class RawKVClient implements RawKVClientBase {
  public static final Histogram RAW_REQUEST_LATENCY =
      HistogramUtils.buildDuration()
          .name("client_java_raw_requests_latency")
          .help("client raw request latency.")
          .labelNames("type", "cluster")
          .register();
  public static final Counter RAW_REQUEST_SUCCESS =
      Counter.build()
          .name("client_java_raw_requests_success")
          .help("client raw request success.")
          .labelNames("type", "cluster")
          .register();
  public static final Counter RAW_REQUEST_FAILURE =
      Counter.build()
          .name("client_java_raw_requests_failure")
          .help("client raw request failure.")
          .labelNames("type", "cluster")
          .register();

  public static final List<Batch> EMPTY_BATCH_LIST = new ArrayList<>();
  public static final List<WriteBatch> EMPTY_WRITEBATCH_LIST = new ArrayList<>();
  public static final List<DeleteRange> EMPTY_DELETERANGE_LIST = new ArrayList<>();
  public static final List<CoprocessorRange> EMPTY_COPROCESSORRANGE_LIST = new ArrayList<>();
  private static final Logger logger = LoggerFactory.getLogger(RawKVClient.class);
  private static final TiKVException ERR_MAX_SCAN_LIMIT_EXCEEDED =
      new TiKVException("limit should be less than MAX_RAW_SCAN_LIMIT");

  /** TiKV cluster id */
  private final Long clusterId;
  /** The address of TiKV pd */
  private final List<URI> pdAddresses;
  /** The session information */
  private final TiSession tiSession;
  /** TiKV Region/Store manager */
  private final RegionManager regionManager;
  /** Client build context */
  private final RegionStoreClient.RegionStoreClientBuilder clientBuilder;
  /** Client configuration */
  private final TiConfiguration conf;
  /** Is enable atomic for cas operation */
  private final boolean atomicForCAS;
  /** The send request/receive response worker pool of batch get operation */
  private final ExecutorService batchGetThreadPool;
  /** The send request/receive response worker pool of batch put operation */
  private final ExecutorService batchPutThreadPool;
  /** The send request/receive response worker pool of batch delete operation */
  private final ExecutorService batchDeleteThreadPool;
  /** The send request/receive response worker pool of batch scan operation */
  private final ExecutorService batchScanThreadPool;
  /** The send request/receive response worker pool of delete range operation */
  private final ExecutorService deleteRangeThreadPool;

  /** The send request/receive response worker pool of coprocessor operation */
  private final ExecutorService coprocessorThreadPool;

  /** Store client manager */
  private final StoreClientManager storeClientManager;

  /** Operation labels */
  private final Map<String, String[]> labels = new HashMap<>();

  /** Unrecoverable grpc error code, need not retry */
  public static final ImmutableSet<Status.Code> unrecoverableStatus =
      ImmutableSet.of(
          Status.Code.ALREADY_EXISTS, Status.Code.PERMISSION_DENIED,
          Status.Code.INVALID_ARGUMENT, Status.Code.NOT_FOUND,
          Status.Code.UNIMPLEMENTED, Status.Code.OUT_OF_RANGE,
          Status.Code.UNAUTHENTICATED, Status.Code.CANCELLED);

  public RawKVClient(TiSession session, RegionStoreClient.RegionStoreClientBuilder clientBuilder) {
    Objects.requireNonNull(session, "session is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = session.getConf();
    this.tiSession = session;
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = session.getThreadPoolForBatchGet();
    this.batchPutThreadPool = session.getThreadPoolForBatchPut();
    this.batchDeleteThreadPool = session.getThreadPoolForBatchDelete();
    this.batchScanThreadPool = session.getThreadPoolForBatchScan();
    this.deleteRangeThreadPool = session.getThreadPoolForDeleteRange();
    this.coprocessorThreadPool = session.getThreadPoolForCoprocessor();
    this.atomicForCAS = conf.isEnableAtomicForCAS();
    this.clusterId = session.getPDClient().getClusterId();
    this.pdAddresses = session.getPDClient().getPdAddrs();
    this.regionManager = clientBuilder.getRegionManager();
    this.storeClientManager = session.getStoreClientManager();
    prepareLabels();
  }

  private void prepareLabels() {
    labels.put("client_raw_put", new String[] {"client_raw_put", clusterId.toString()});
    labels.put("client_raw_batch_put", new String[] {"client_raw_batch_put", clusterId.toString()});
    labels.put(
        "client_raw_compare_and_set",
        new String[] {"client_raw_compare_and_set", clusterId.toString()});

    labels.put(
        "client_raw_batch_write", new String[] {"client_raw_batch_write", clusterId.toString()});
    labels.put("client_raw_batch_get", new String[] {"client_raw_batch_get", clusterId.toString()});
    labels.put("client_raw_get", new String[] {"client_raw_get", clusterId.toString()});

    labels.put(
        "client_raw_batch_delete", new String[] {"client_raw_batch_delete", clusterId.toString()});
    labels.put(
        "client_raw_get_key_ttl", new String[] {"client_raw_batch_get", clusterId.toString()});
    labels.put(
        "client_raw_set_key_ttl", new String[] {"client_raw_set_key_ttl", clusterId.toString()});

    labels.put(
        "client_raw_batch_scan", new String[] {"client_raw_batch_scan", clusterId.toString()});
    labels.put("client_raw_scan", new String[] {"client_raw_scan", clusterId.toString()});
    labels.put(
        "client_raw_scan_without_limit",
        new String[] {"client_raw_scan_without_limit", clusterId.toString()});

    labels.put("client_raw_delete", new String[] {"client_raw_delete", clusterId.toString()});
    labels.put(
        "client_raw_delete_range", new String[] {"client_raw_delete_range", clusterId.toString()});

    labels.put(
        "client_raw_coprocessor", new String[] {"client_raw_coprocessor", clusterId.toString()});

    labels.put("client_raw_count", new String[] {"client_raw_count", clusterId.toString()});
  }

  private String[] withClusterId(String label) {
    return labels.get(label);
  }

  private SlowLog withClusterInfo(SlowLog logger) {
    return logger.withField("cluster_id", clusterId).withField("pd_addresses", pdAddresses);
  }

  @Override
  public void put(ByteString key, ByteString value) {
    put(key, value, 0, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void put(ByteString key, ByteString value, long ttl) {
    put(key, value, ttl, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void put(ByteString key, ByteString value, String cf) {
    put(key, value, 0, cf);
  }

  @Override
  public void put(ByteString key, ByteString value, long ttl, String cf) {
    String[] labels = withClusterId("client_raw_put");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("put");
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog, clusterId);

    try {
      while (true) {
        TiRegion region = null;
        TiStore store = null;
        try {
          Pair<TiRegion, TiStore> pair =
              regionManager.getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
          region = pair.first;
          store = pair.second;
          ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);

          StoreClient storeClient = storeClientManager.getStoreClient(pair.second);
          storeClient.rawPut(region, backOffer, key, value, ttl, atomicForCAS, cf);
          RAW_REQUEST_SUCCESS.labels(labels).inc();
          return;
        } catch (NeedRetryException e) {
          ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
        } catch (NeedResplitAndRetryException e) {
          ExceptionHandlerUtils.handleNeedResplitAndRetryException(
              e, null, region, backOffer, regionManager);
        } catch (NeedNotRetryException e) {
          ExceptionHandlerUtils.handleNeedNotRetryException(e);
        } catch (final TiKVException e) {
          ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
        } catch (StatusRuntimeException e) {
          // Rpc error
          ExceptionHandlerUtils.handleStatusRuntimeException(
              e, null, store, region, backOffer, regionManager, storeClientManager);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value) {
    return putIfAbsent(key, value, 0, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value, long ttl) {
    return putIfAbsent(key, value, ttl, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value, String cf) {
    return putIfAbsent(key, value, 0, cf);
  }

  @Override
  public Optional<ByteString> putIfAbsent(ByteString key, ByteString value, long ttl, String cf) {
    try {
      compareAndSet(key, Optional.empty(), value, ttl, cf);
      return Optional.empty();
    } catch (RawCASConflictException e) {
      return e.getPrevValue();
    }
  }

  @Override
  public void compareAndSet(ByteString key, Optional<ByteString> prevValue, ByteString value) {
    compareAndSet(key, prevValue, value, 0, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void compareAndSet(
      ByteString key, Optional<ByteString> prevValue, ByteString value, long ttl) {
    compareAndSet(key, prevValue, value, ttl, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void compareAndSet(
      ByteString key, Optional<ByteString> prevValue, ByteString value, String cf) {
    compareAndSet(key, prevValue, value, 0, cf);
  }

  @Override
  public void compareAndSet(
      ByteString key, Optional<ByteString> prevValue, ByteString value, long ttl, String cf) {
    if (!atomicForCAS) {
      throw new IllegalArgumentException(
          "To use compareAndSet or putIfAbsent, please enable the config tikv.enable_atomic_for_cas.");
    }

    String[] labels = withClusterId("client_raw_compare_and_set");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("putIfAbsent");
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog, clusterId);
    try {
      while (true) {
        TiRegion region = null;
        TiStore store = null;
        try {
          Pair<TiRegion, TiStore> pair =
              regionManager.getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
          region = pair.first;
          store = pair.second;
          ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);

          StoreClient storeClient = storeClientManager.getStoreClient(store);
          storeClient.rawCompareAndSet(region, backOffer, key, prevValue, value, ttl, cf);
          RAW_REQUEST_SUCCESS.labels(labels).inc();
          return;
        } catch (NeedRetryException e) {
          ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
        } catch (NeedResplitAndRetryException e) {
          ExceptionHandlerUtils.handleNeedResplitAndRetryException(
              e, null, region, backOffer, regionManager);
        } catch (NeedNotRetryException e) {
          ExceptionHandlerUtils.handleNeedNotRetryException(e);
        } catch (final TiKVException e) {
          ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
        } catch (StatusRuntimeException e) {
          // Rpc error
          ExceptionHandlerUtils.handleStatusRuntimeException(
              e, null, store, region, backOffer, regionManager, storeClientManager);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs) {
    batchPut(kvPairs, 0, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl) {
    batchPut(kvPairs, ttl, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs, String cf) {
    batchPut(kvPairs, 0, cf);
  }

  @Override
  public void batchPut(Map<ByteString, ByteString> kvPairs, long ttl, String cf) {
    String[] labels = withClusterId("client_raw_batch_put");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchPut");
    span.addProperty("keySize", String.valueOf(kvPairs.size()));

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVBatchWriteTimeoutInMS(), slowLog, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchPut(backOffer, kvPairs, ttl, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private void doSendBatchPut(
      BackOffer backOffer,
      Map<ByteString, ByteString> kvPairs,
      long ttl,
      long deadline,
      String cf) {

    // Split the data
    List<Batch> batches = split(kvPairs, backOffer);

    List<Future<List<Batch>>> futureList = new ArrayList<>(batches.size());
    Queue<List<Batch>> taskQueue = new LinkedList<>();
    taskQueue.offer(batches);

    ExecutorCompletionService<List<Batch>> completionService =
        new ExecutorCompletionService<>(batchPutThreadPool);
    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchPutInBatchesWithRetry(batch.getBackOffer(), batch, ttl, cf)));
      }

      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<Batch>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private List<Batch> split(Map<ByteString, ByteString> kvPairs, BackOffer backOffer) {
    // Group kvs by region
    // long startTs = System.currentTimeMillis();
    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(regionManager, kvPairs.keySet(), backOffer);
    // logger.info("Group by use time = " + (System.currentTimeMillis() - startTs));

    // Split data in each region
    List<Batch> batches = new ArrayList<>(groupKeys.size());
    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          backOffer,
          batches,
          entry.getKey(),
          entry.getValue(),
          entry.getValue().stream().map(kvPairs::get).collect(Collectors.toList()),
          RAW_BATCH_PUT_SIZE,
          MAX_RAW_BATCH_LIMIT);
    }

    return batches;
  }

  private List<Batch> doSendBatchPutInBatchesWithRetry(
      BackOffer backOffer, Batch batch, long ttl, String cf) {
    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(batch, backOffer);
        store = storeAndClient.first;
        storeAndClient.second.rawBatchPut(
            batch.getRegion(), backOffer, batch, ttl, atomicForCAS, cf);
        return EMPTY_BATCH_LIST;
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, batch, batch.getRegion(), regionManager);
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (StatusRuntimeException e) {
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, batch, store, batch.getRegion(), backOffer, regionManager, storeClientManager);
      } catch (NeedResplitAndRetryException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest", e);
        // retry
        return split(batch.getMap(), backOffer);
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      }
    }
  }

  @Override
  public void batchWrite(List<Kvrpcpb.WriteOp> batch, long ttl) {
    batchWrite(batch, ttl, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void batchWrite(List<Kvrpcpb.WriteOp> batch, long ttl, String cf) {
    String[] labels = withClusterId("client_raw_batch_write");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchWrite");
    span.addProperty("keySize", String.valueOf(batch.size()));

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVBatchWriteTimeoutInMS(), slowLog, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchWrite(backOffer, batch, ttl, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private void doSendBatchWrite(
      BackOffer backOffer, List<Kvrpcpb.WriteOp> writeOps, long ttl, long deadline, String cf) {
    // Split first
    List<WriteBatch> batches = splitWriteOps(writeOps, backOffer);

    List<Future<List<WriteBatch>>> futureList = new ArrayList<>(batches.size());
    Queue<List<WriteBatch>> taskQueue = new LinkedList<>();
    taskQueue.offer(batches);

    ExecutorCompletionService<List<WriteBatch>> completionService =
        new ExecutorCompletionService<>(batchPutThreadPool);
    while (!taskQueue.isEmpty()) {
      List<WriteBatch> task = taskQueue.poll();
      for (WriteBatch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchWriteInBatchesWithRetry(batch.getBackOffer(), batch, ttl, cf)));
      }

      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<WriteBatch>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private List<WriteBatch> splitWriteOps(List<Kvrpcpb.WriteOp> writeOps, BackOffer backOffer) {
    // long startTs = System.currentTimeMillis();
    Map<TiRegion, List<Kvrpcpb.WriteOp>> groupKeys =
        groupWriteOpsByRegion(regionManager, writeOps, backOffer);
    // logger.info("Group by use time = " + (System.currentTimeMillis() - startTs));
    List<WriteBatch> batches = new ArrayList<>(groupKeys.size());

    for (Map.Entry<TiRegion, List<Kvrpcpb.WriteOp>> entry : groupKeys.entrySet()) {
      appendWriteBatches(
          backOffer,
          batches,
          entry.getKey(),
          entry.getValue(),
          RAW_BATCH_PUT_SIZE,
          MAX_RAW_BATCH_LIMIT);
    }

    return batches;
  }

  private List<WriteBatch> doSendBatchWriteInBatchesWithRetry(
      BackOffer backOffer, WriteBatch batch, long ttl, String cf) {
    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(batch, backOffer);
        store = storeAndClient.first;
        storeAndClient.second.rawBatchWrite(
            batch.getRegion(), backOffer, batch, ttl, atomicForCAS, cf);
        return EMPTY_WRITEBATCH_LIST;
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, batch, batch.getRegion(), regionManager);
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (StatusRuntimeException e) {
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, batch, store, batch.getRegion(), backOffer, regionManager, storeClientManager);
      } catch (NeedResplitAndRetryException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest", e);
        // retry
        return splitWriteOps(batch.getWriteOps(), backOffer);
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      }
    }
  }

  @Override
  public Optional<ByteString> get(ByteString key) {
    return get(key, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> batchGet(List<ByteString> keys) {
    return batchGet(keys, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> batchGet(List<ByteString> keys, String cf) {
    String[] labels = withClusterId("client_raw_batch_get");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchGet");
    span.addProperty("keySize", String.valueOf(keys.size()));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVBatchReadTimeoutInMS(), slowLog, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchReadTimeoutInMS();
      List<Kvrpcpb.KvPair> result = doSendBatchGet(backOffer, keys, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private List<Kvrpcpb.KvPair> doSendBatchGet(
      BackOffer backOffer, List<ByteString> keys, long deadline, String cf) {
    ExecutorCompletionService<Pair<List<Batch>, List<Kvrpcpb.KvPair>>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);

    // Split the keys
    List<Batch> batches = split(keys, backOffer, RAW_BATCH_GET_SIZE, MAX_RAW_BATCH_LIMIT);

    Queue<List<Batch>> taskQueue = new LinkedList<>();
    List<Kvrpcpb.KvPair> result = new ArrayList<>(keys.size());
    taskQueue.offer(batches);

    List<Future<Pair<List<Batch>, List<Kvrpcpb.KvPair>>>> futureList =
        new ArrayList<>(batches.size());
    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchGetInBatchesWithRetry(batch.getBackOffer(), batch, cf)));
      }
      try {
        result.addAll(
            getTasksWithOutput(
                completionService, taskQueue, task, deadline - System.currentTimeMillis()));
      } catch (Exception e) {
        for (Future<Pair<List<Batch>, List<Kvrpcpb.KvPair>>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }

    return result;
  }

  private Pair<List<Batch>, List<Kvrpcpb.KvPair>> doSendBatchGetInBatchesWithRetry(
      BackOffer backOffer, Batch batch, String cf) {

    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(batch, backOffer);
        store = storeAndClient.first;
        return Pair.create(
            EMPTY_BATCH_LIST,
            storeAndClient.second.rawBatchGet(batch.getRegion(), backOffer, batch.getKeys(), cf));
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, batch, batch.getRegion(), regionManager);
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (StatusRuntimeException e) {
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, batch, store, batch.getRegion(), backOffer, regionManager, storeClientManager);
      } catch (NeedResplitAndRetryException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest", e);
        // retry
        return Pair.create(
            split(batch.getKeys(), backOffer, RAW_BATCH_GET_SIZE, MAX_RAW_BATCH_LIMIT),
            new ArrayList<>());
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      }
    }
  }

  private List<Batch> split(
      List<ByteString> keys, BackOffer backOffer, int sizeLimit, int batchSizeLimit) {
    Map<TiRegion, List<ByteString>> groupKeys =
        groupKeysByRegion(regionManager, keys, backOffer, false);

    List<Batch> batches = new ArrayList<>(groupKeys.size());
    for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
      appendBatches(
          backOffer, batches, entry.getKey(), entry.getValue(), sizeLimit, batchSizeLimit);
    }
    return batches;
  }

  @Override
  public Optional<ByteString> get(ByteString key, String cf) {
    String[] labels = withClusterId("client_raw_get");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();

    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("get");

    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog, clusterId);
    try {
      while (true) {
        TiStore store = null;
        TiRegion region = null;
        try {
          Pair<TiRegion, TiStore> pair =
              regionManager.getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
          store = pair.second;
          region = pair.first;
          ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);

          StoreClient storeClient = storeClientManager.getStoreClient(store);
          Optional<ByteString> result = storeClient.rawGet(region, backOffer, key, cf);
          RAW_REQUEST_SUCCESS.labels(labels).inc();
          return result;
        } catch (NeedRetryException e) {
          ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
        } catch (NeedResplitAndRetryException e) {
          ExceptionHandlerUtils.handleNeedResplitAndRetryException(
              e, null, region, backOffer, regionManager);
        } catch (NeedNotRetryException e) {
          ExceptionHandlerUtils.handleNeedNotRetryException(e);
        } catch (StatusRuntimeException e) {
          ExceptionHandlerUtils.handleStatusRuntimeException(
              e, null, store, region, backOffer, regionManager, storeClientManager);
        } catch (final TiKVException e) {
          ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public void batchDelete(List<ByteString> keys) {
    batchDelete(keys, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void batchDelete(List<ByteString> keys, String cf) {
    String[] labels = withClusterId("client_raw_batch_delete");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("batchDelete");
    span.addProperty("keySize", String.valueOf(keys.size()));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVBatchWriteTimeoutInMS(), slowLog, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVBatchWriteTimeoutInMS();
      doSendBatchDelete(backOffer, keys, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private void doSendBatchDelete(
      BackOffer backOffer, List<ByteString> keys, long deadline, String cf) {
    ExecutorCompletionService<List<Batch>> completionService =
        new ExecutorCompletionService<>(batchDeleteThreadPool);

    List<Batch> batches = split(keys, backOffer, RAW_BATCH_DELETE_SIZE, MAX_RAW_BATCH_LIMIT);

    Queue<List<Batch>> taskQueue = new LinkedList<>();
    taskQueue.offer(batches);

    List<Future<List<Batch>>> futureList = new ArrayList<>(batches.size());
    while (!taskQueue.isEmpty()) {
      List<Batch> task = taskQueue.poll();
      for (Batch batch : task) {
        futureList.add(
            completionService.submit(
                () -> doSendBatchDeleteInBatchesWithRetry(batch.getBackOffer(), batch, cf)));
      }
      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<Batch>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private Pair<TiStore, StoreClient> getStoreClient(RegionBatch batch, BackOffer backOffer) {
    Pair<TiRegion, TiStore> storeAndRegion =
        regionManager.getStoreByRegion(batch.getRegion(), TiStoreType.TiKV, backOffer);
    TiStore store = storeAndRegion.second;
    batch.setRegion(storeAndRegion.first);
    ExceptionHandlerUtils.checkStore(store, batch.getRegion(), backOffer, regionManager);
    StoreClient storeClient = storeClientManager.getStoreClient(store);
    return Pair.create(store, storeClient);
  }

  private List<Batch> doSendBatchDeleteInBatchesWithRetry(
      BackOffer backOffer, Batch batch, String cf) {
    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(batch, backOffer);
        store = storeAndClient.first;
        storeAndClient.second.rawBatchDelete(
            batch.getRegion(), backOffer, batch.getKeys(), atomicForCAS, cf);
        return EMPTY_BATCH_LIST;
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, batch, batch.getRegion(), regionManager);
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (StatusRuntimeException e) {
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, batch, store, batch.getRegion(), backOffer, regionManager, storeClientManager);
      } catch (NeedResplitAndRetryException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest", e);
        // retry
        return split(batch.getKeys(), backOffer, RAW_BATCH_DELETE_SIZE, MAX_RAW_BATCH_LIMIT);
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      }
    }
  }

  @Override
  public Optional<Long> getKeyTTL(ByteString key) {
    return Optional.empty();
  }

  @Override
  public Optional<Long> getKeyTTL(ByteString key, String cf) {
    String[] labels = withClusterId("client_raw_get_key_ttl");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("getKeyTTL");
    span.addProperty("key", KeyUtils.formatBytesUTF8(key));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog, clusterId);
    try {
      while (true) {
        TiStore store = null;
        TiRegion region = null;
        try {
          Pair<TiRegion, TiStore> pair =
              regionManager.getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
          store = pair.second;
          region = pair.first;
          ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);
          StoreClient storeClient = storeClientManager.getStoreClient(store);
          Optional<Long> result = storeClient.rawGetKeyTTL(region, backOffer, key, cf);
          RAW_REQUEST_SUCCESS.labels(labels).inc();
          return result;
        } catch (NeedRetryException e) {
          ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
        } catch (NeedResplitAndRetryException e) {
          ExceptionHandlerUtils.handleNeedResplitAndRetryException(
              e, null, region, backOffer, regionManager);
        } catch (NeedNotRetryException e) {
          ExceptionHandlerUtils.handleNeedNotRetryException(e);
        } catch (StatusRuntimeException e) {
          ExceptionHandlerUtils.handleStatusRuntimeException(
              e, null, store, region, backOffer, regionManager, storeClientManager);
        } catch (final TiKVException e) {
          ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public void setKeyTTL(ByteString key, long ttl) {}

  @Override
  public void setKeyTTL(ByteString key, long ttl, String cf) {
    String[] labels = withClusterId("client_raw_set_key_ttl");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("setKeyTTL");
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVReadTimeoutInMS(), slowLog, clusterId);
    try {
      while (true) {
        TiRegion region = null;
        TiStore store = null;
        try {
          Pair<TiRegion, TiStore> pair =
              regionManager.getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
          region = pair.first;
          store = pair.second;
          ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);
          StoreClient storeClient = storeClientManager.getStoreClient(store);
          storeClient.rawSetKeyTTL(region, backOffer, key, ttl, cf);
          RAW_REQUEST_SUCCESS.labels(labels).inc();
          return;
        } catch (NeedRetryException e) {
          ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
        } catch (NeedResplitAndRetryException e) {
          ExceptionHandlerUtils.handleNeedResplitAndRetryException(
              e, null, region, backOffer, regionManager);
        } catch (NeedNotRetryException e) {
          ExceptionHandlerUtils.handleNeedNotRetryException(e);
        } catch (final TiKVException e) {
          ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
        } catch (StatusRuntimeException e) {
          // Rpc error
          ExceptionHandlerUtils.handleStatusRuntimeException(
              e, null, store, region, backOffer, regionManager, storeClientManager);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public List<ByteString> coprocessor(
      ByteString request, ByteString startKey, ByteString endKey, String cf) {
    String[] labels = withClusterId("client_raw_coprocessor");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("coprocessor");
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVCoprocessorTimeoutInMS(), slowLog, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVCoprocessorTimeoutInMS();
      List<ByteString> result =
          doCoprocessorSend(backOffer, request, startKey, endKey, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public long count(ByteString startKey, ByteString endKey, String cf) {
    String[] labels = withClusterId("client_raw_count");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVBatchReadSlowLogInMS()));
    SlowLogSpan span = slowLog.start("count");
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVCoprocessorTimeoutInMS(), slowLog, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVCoprocessorTimeoutInMS();
      List<Long> subResults = doCountSend(backOffer, startKey, endKey, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      long result = 0;
      for (long subResult : subResults) {
        result += subResult;
      }
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private List<Long> doCountSend(
      ConcreteBackOffer backOffer,
      ByteString startKey,
      ByteString endKey,
      long deadline,
      String cf) {
    ExecutorCompletionService<Pair<List<CoprocessorRange>, List<Long>>> completionService =
        new ExecutorCompletionService<>(coprocessorThreadPool);

    List<Future<Pair<List<CoprocessorRange>, List<Long>>>> futureList = new ArrayList<>();

    List<TiRegion> regions = fetchRegionsFromRange(backOffer, startKey, endKey);
    List<CoprocessorRange> ranges = new ArrayList<>();
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      ByteString start = calcKeyByCondition(i == 0, startKey, region.getStartKey());
      ByteString end = calcKeyByCondition(i == regions.size() - 1, endKey, region.getEndKey());
      ranges.add(new CoprocessorRange(backOffer, region, start, end));
    }
    Queue<List<CoprocessorRange>> taskQueue = new LinkedList<>();
    taskQueue.offer(ranges);
    List<Long> results = new ArrayList<>(ranges.size());

    while (!taskQueue.isEmpty()) {
      List<CoprocessorRange> task = taskQueue.poll();
      for (CoprocessorRange range : task) {
        futureList.add(
            completionService.submit(
                () ->
                    doSendCountWithRetry(
                        range.getBackOffer(),
                        new RegionBatch(backOffer, range.getRegion()),
                        range,
                        cf)));
      }
      try {
        results.addAll(
            getTasksWithOutput(
                completionService, taskQueue, task, deadline - System.currentTimeMillis()));
      } catch (Exception e) {
        for (Future<Pair<List<CoprocessorRange>, List<Long>>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }

    return results;
  }

  private ByteString calcKeyByCondition(boolean condition, ByteString key1, ByteString key2) {
    if (condition) {
      return key1;
    }
    return key2;
  }

  private Pair<List<CoprocessorRange>, List<Long>> doSendCountWithRetry(
      BackOffer backOffer, RegionBatch batch, CoprocessorRange range, String cf) {

    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(batch, backOffer);
        store = storeAndClient.first;

        ByteString startKey = range.getStartKey();
        ByteString endKey = range.getEndKey();
        long count = 0;
        while (true) {
          Kvrpcpb.KeyRange tikvRange =
              Kvrpcpb.KeyRange.newBuilder().setStartKey(startKey).setEndKey(endKey).build();
          int countLimit = conf.getRawKVCountLimit();
          Pair<Long, ByteString> countRet =
              storeAndClient.second.rawCountV2(
                  batch.getRegion(), backOffer, tikvRange, cf, countLimit);
          count += countRet.first;
          if (countRet.first < countLimit) {
            // Scan over
            break;
          }

          // Generate start key for next scan
          byte[] lastKey = countRet.second.toByteArray();
          startKey = ByteString.copyFrom(Arrays.copyOf(lastKey, lastKey.length + 1));
        }

        // List<Kvrpcpb.KeyRange> ranges = new ArrayList<>(1);
        // ranges.add(
        //    Kvrpcpb.KeyRange.newBuilder()
        //        .setStartKey(range.getStartKey())
        //        .setEndKey(range.getEndKey())
        //        .build());
        // results.add(storeAndClient.second.rawCount(batch.getRegion(), backOffer, ranges, cf));

        List<Long> results = new ArrayList<>(1);
        results.add(count);
        return Pair.create(EMPTY_COPROCESSORRANGE_LIST, results);
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, batch, batch.getRegion(), regionManager);
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (StatusRuntimeException e) {
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, batch, store, batch.getRegion(), backOffer, regionManager, storeClientManager);
      } catch (NeedResplitAndRetryException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest", e);
        // retry
        ExceptionHandlerUtils.handleNeedResplitAndRetryException(
            e, batch, batch.getRegion(), backOffer, regionManager);
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      }
    }
  }

  private List<ByteString> doCoprocessorSend(
      ConcreteBackOffer backOffer,
      ByteString request,
      ByteString startKey,
      ByteString endKey,
      long deadline,
      String cf) {
    ExecutorCompletionService<Pair<List<CoprocessorRange>, List<ByteString>>> completionService =
        new ExecutorCompletionService<>(coprocessorThreadPool);

    List<Future<Pair<List<CoprocessorRange>, List<ByteString>>>> futureList = new ArrayList<>();

    List<TiRegion> regions = fetchRegionsFromRange(backOffer, startKey, endKey);
    List<CoprocessorRange> ranges = new ArrayList<>();
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      ByteString start = calcKeyByCondition(i == 0, startKey, region.getStartKey());
      ByteString end = calcKeyByCondition(i == regions.size() - 1, endKey, region.getEndKey());
      ranges.add(new CoprocessorRange(backOffer, region, start, end));
    }
    Queue<List<CoprocessorRange>> taskQueue = new LinkedList<>();
    taskQueue.offer(ranges);
    List<ByteString> results = new ArrayList<>(ranges.size());

    while (!taskQueue.isEmpty()) {
      List<CoprocessorRange> task = taskQueue.poll();
      for (CoprocessorRange range : task) {
        futureList.add(
            completionService.submit(
                () ->
                    doSendCoprocessorWithRetry(
                        range.getBackOffer(),
                        new RegionBatch(backOffer, range.getRegion()),
                        request,
                        range,
                        cf)));
      }
      try {
        results.addAll(
            getTasksWithOutput(
                completionService, taskQueue, task, deadline - System.currentTimeMillis()));
      } catch (Exception e) {
        for (Future<Pair<List<CoprocessorRange>, List<ByteString>>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }

    return results;
  }

  private Pair<List<CoprocessorRange>, List<ByteString>> doSendCoprocessorWithRetry(
      BackOffer backOffer,
      RegionBatch batch,
      ByteString request,
      CoprocessorRange range,
      String cf) {

    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(batch, backOffer);
        store = storeAndClient.first;
        List<Kvrpcpb.KeyRange> ranges = new ArrayList<>(1);
        ranges.add(
            Kvrpcpb.KeyRange.newBuilder()
                .setStartKey(range.getStartKey())
                .setEndKey(range.getEndKey())
                .build());
        List<ByteString> results = new ArrayList<>(1);
        results.add(
            storeAndClient.second.rawCoprocessor(
                batch.getRegion(),
                backOffer,
                CoprocessorUtils.COPROCESSOR_PLUGIN_NAME,
                CoprocessorUtils.COPROCESSOR_PLUGIN_VERSION,
                request,
                ranges));

        return Pair.create(EMPTY_COPROCESSORRANGE_LIST, results);
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, batch, batch.getRegion(), regionManager);
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (StatusRuntimeException e) {
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, batch, store, batch.getRegion(), backOffer, regionManager, storeClientManager);
      } catch (NeedResplitAndRetryException e) {
        // TODO: any elegant way to re-split the ranges if fails?
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        logger.warn("ReSplitting ranges for BatchPutRequest", e);
        // retry
        ExceptionHandlerUtils.handleNeedResplitAndRetryException(
            e, batch, batch.getRegion(), backOffer, regionManager);
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      }
    }
  }

  @Override
  public List<List<ByteString>> batchScanKeys(
      List<Pair<ByteString, ByteString>> ranges, int eachLimit) {
    return batchScanKeys(ranges, eachLimit, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<List<ByteString>> batchScanKeys(
      List<Pair<ByteString, ByteString>> ranges, int eachLimit, String cf) {
    return batchScan(
            ranges
                .stream()
                .map(
                    range ->
                        ScanOption.newBuilder()
                            .setStartKey(range.first)
                            .setEndKey(range.second)
                            .setCf(cf)
                            .setLimit(eachLimit)
                            .setKeyOnly(true)
                            .build())
                .collect(Collectors.toList()))
        .stream()
        .map(kvs -> kvs.stream().map(Kvrpcpb.KvPair::getKey).collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  @Override
  public List<List<Kvrpcpb.KvPair>> batchScan(List<ScanOption> ranges) {
    return batchScan(ranges, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<List<Kvrpcpb.KvPair>> batchScan(List<ScanOption> ranges, String cf) {
    String[] labels = withClusterId("client_raw_batch_scan");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    long deadline = System.currentTimeMillis() + conf.getRawKVScanTimeoutInMS();
    List<Future<Pair<Integer, List<Kvrpcpb.KvPair>>>> futureList = new ArrayList<>();
    try {
      if (ranges.isEmpty()) {
        return new ArrayList<>();
      }
      ExecutorCompletionService<Pair<Integer, List<Kvrpcpb.KvPair>>> completionService =
          new ExecutorCompletionService<>(batchScanThreadPool);
      int num = 0;
      for (ScanOption scanOption : ranges) {
        int i = num;
        futureList.add(completionService.submit(() -> Pair.create(i, scan(scanOption, cf))));
        ++num;
      }
      List<List<Kvrpcpb.KvPair>> scanResults = new ArrayList<>();
      for (int i = 0; i < num; i++) {
        scanResults.add(new ArrayList<>());
      }
      for (int i = 0; i < num; i++) {
        try {
          Future<Pair<Integer, List<Kvrpcpb.KvPair>>> future =
              completionService.poll(deadline - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
          if (future == null) {
            throw new TiKVException("TimeOut Exceeded for current operation.");
          }
          Pair<Integer, List<Kvrpcpb.KvPair>> scanResult = future.get();
          scanResults.set(scanResult.first, scanResult.second);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new TiKVException("Current thread interrupted.", e);
        } catch (ExecutionException e) {
          throw new TiKVException("Execution exception met.", e);
        }
      }
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      return scanResults;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      for (Future<Pair<Integer, List<Kvrpcpb.KvPair>>> future : futureList) {
        future.cancel(true);
      }
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit) {
    return scan(startKey, endKey, limit, false, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, int limit, String cf) {
    return scan(startKey, endKey, limit, false, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    return scan(startKey, endKey, limit, keyOnly, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly, String cf) {
    String[] labels = withClusterId("client_raw_scan");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVScanSlowLogInMS()));
    SlowLogSpan span = slowLog.start("scan");
    span.addProperty("startKey", KeyUtils.formatBytesUTF8(startKey));
    span.addProperty("endKey", KeyUtils.formatBytesUTF8(endKey));
    span.addProperty("limit", String.valueOf(limit));
    span.addProperty("keyOnly", String.valueOf(keyOnly));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVScanTimeoutInMS(), slowLog, clusterId);
    try {
      List<Kvrpcpb.KvPair> result;

      // Check if start key and end key are in the same region
      if (limit > 0
          && startKey != null
          && !startKey.isEmpty()
          && endKey != null
          && !endKey.isEmpty()) {

        while (true) {
          TiRegion startRegion = clientBuilder.getRegionByKey(startKey);
          TiRegion endRegion = clientBuilder.getRegionByKey(endKey);
          if (startRegion != null
              && endRegion != null
              && startRegion.getId() == endRegion.getId()) {
            TiStore store = null;
            TiRegion region = null;
            try {
              Pair<TiRegion, TiStore> pair =
                  regionManager.getRegionStorePairByKey(startKey, TiStoreType.TiKV, backOffer);
              store = pair.second;
              region = pair.first;

              ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);
              StoreClient storeClient = storeClientManager.getStoreClient(store);
              result = storeClient.rawScan(region, backOffer, startKey, endKey, limit, keyOnly, cf);
              RAW_REQUEST_SUCCESS.labels(labels).inc();
              return result;
            } catch (NeedRetryException e) {
              ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
            } catch (NeedNotRetryException e) {
              ExceptionHandlerUtils.handleNeedNotRetryException(e);
            } catch (StatusRuntimeException e) {
              ExceptionHandlerUtils.handleStatusRuntimeException(
                  e, null, store, region, backOffer, regionManager, storeClientManager);
            } catch (NeedResplitAndRetryException e) {
              // TODO: any elegant way to re-split the ranges if fails?
              backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
            } catch (final TiKVException e) {
              ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
            }
          } else {
            break;
          }
        }
      }

      if (conf.getScanPreallocateEnable() && limit > 0) {
        result = new ArrayList<>(limit);
      } else {
        result = new ArrayList<>();
      }

      ScanIterator iterator =
          rawScanIterator(conf, clientBuilder, startKey, endKey, limit, keyOnly, backOffer, cf);

      iterator.forEachRemaining(result::add);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private ScanIterator rawScanIterator(
      TiConfiguration conf,
      RegionStoreClient.RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      int limit,
      boolean keyOnly,
      BackOffer backOffer,
      String cf) {
    if (limit > MAX_RAW_SCAN_LIMIT) {
      throw ERR_MAX_SCAN_LIMIT_EXCEEDED;
    }
    return new RawScanIteratorV2(
        conf,
        builder,
        startKey,
        endKey,
        limit,
        keyOnly,
        backOffer,
        cf,
        regionManager,
        storeClientManager);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit) {
    return scan(startKey, ByteString.EMPTY, limit, false, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit, String cf) {
    return scan(startKey, ByteString.EMPTY, limit, false, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit, boolean keyOnly) {
    return scan(startKey, ByteString.EMPTY, limit, keyOnly, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, int limit, boolean keyOnly, String cf) {
    return scan(startKey, ByteString.EMPTY, limit, keyOnly, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey) {
    return scan(startKey, endKey, false, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, String cf) {
    return scan(startKey, endKey, false, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(ByteString startKey, ByteString endKey, boolean keyOnly) {
    return scan(startKey, endKey, keyOnly, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scan(
      ByteString startKey, ByteString endKey, boolean keyOnly, String cf) {
    String[] labels = withClusterId("client_raw_scan_without_limit");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVScanSlowLogInMS()));
    SlowLogSpan span = slowLog.start("scan");
    span.addProperty("startKey", KeyUtils.formatBytesUTF8(startKey));
    span.addProperty("endKey", KeyUtils.formatBytesUTF8(endKey));
    span.addProperty("keyOnly", String.valueOf(keyOnly));
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVScanTimeoutInMS(), slowLog, clusterId);
    try {
      ByteString newStartKey = startKey;
      List<Kvrpcpb.KvPair> result = new ArrayList<>();
      while (true) {
        Iterator<Kvrpcpb.KvPair> iterator =
            rawScanIterator(
                conf,
                clientBuilder,
                newStartKey,
                endKey,
                conf.getScanBatchSize(),
                keyOnly,
                backOffer,
                cf);
        if (!iterator.hasNext()) {
          break;
        }
        iterator.forEachRemaining(result::add);
        newStartKey = Key.toRawKey(result.get(result.size() - 1).getKey()).next().toByteString();
      }
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      return result;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  private List<Kvrpcpb.KvPair> scan(ScanOption scanOption, String cf) {
    ByteString startKey = scanOption.getStartKey();
    ByteString endKey = scanOption.getEndKey();
    int limit = scanOption.getLimit();
    boolean keyOnly = scanOption.isKeyOnly();
    return scan(startKey, endKey, limit, keyOnly, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, int limit, boolean keyOnly) {
    return scan(
        prefixKey,
        Key.toRawKey(prefixKey).nextPrefix().toByteString(),
        limit,
        keyOnly,
        ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey) {
    return scan(
        prefixKey,
        Key.toRawKey(prefixKey).nextPrefix().toByteString(),
        false,
        ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly) {
    return scan(
        prefixKey,
        Key.toRawKey(prefixKey).nextPrefix().toByteString(),
        keyOnly,
        ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(
      ByteString prefixKey, int limit, boolean keyOnly, String cf) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), limit, keyOnly, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, String cf) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), false, cf);
  }

  @Override
  public List<Kvrpcpb.KvPair> scanPrefix(ByteString prefixKey, boolean keyOnly, String cf) {
    return scan(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), keyOnly, cf);
  }

  @Override
  public void delete(ByteString key) {
    delete(key, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public void delete(ByteString key, String cf) {
    String[] labels = withClusterId("client_raw_delete");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    SlowLog slowLog = withClusterInfo(new SlowLogImpl(conf.getRawKVWriteSlowLogInMS()));
    SlowLogSpan span = slowLog.start("delete");
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(conf.getRawKVWriteTimeoutInMS(), slowLog, clusterId);
    try {
      while (true) {
        TiRegion region = null;
        TiStore store = null;
        try {
          Pair<TiRegion, TiStore> pair =
              regionManager.getRegionStorePairByKey(key, TiStoreType.TiKV, backOffer);
          region = pair.first;
          store = pair.second;

          ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);
          StoreClient storeClient = storeClientManager.getStoreClient(store);
          storeClient.rawDelete(region, backOffer, key, atomicForCAS, cf);
          RAW_REQUEST_SUCCESS.labels(labels).inc();
          return;
        } catch (NeedRetryException e) {
          ExceptionHandlerUtils.handleNeedRetryException(e, null, region, regionManager);
        } catch (NeedResplitAndRetryException e) {
          ExceptionHandlerUtils.handleNeedResplitAndRetryException(
              e, null, region, backOffer, regionManager);
        } catch (NeedNotRetryException e) {
          ExceptionHandlerUtils.handleNeedNotRetryException(e);
        } catch (final TiKVException e) {
          ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
        } catch (StatusRuntimeException e) {
          // Rpc error
          ExceptionHandlerUtils.handleStatusRuntimeException(
              e, null, store, region, backOffer, regionManager, storeClientManager);
        }
      }
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      slowLog.setError(e);
      throw e;
    } finally {
      requestTimer.observeDuration();
      span.end();
      slowLog.log();
    }
  }

  @Override
  public void deleteRange(ByteString startKey, ByteString endKey) {
    deleteRange(startKey, endKey, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  @Override
  public synchronized void deleteRange(ByteString startKey, ByteString endKey, String cf) {
    String[] labels = withClusterId("client_raw_delete_range");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    ConcreteBackOffer backOffer =
        ConcreteBackOffer.newDeadlineBackOff(
            conf.getRawKVCleanTimeoutInMS(), SlowLogEmptyImpl.INSTANCE, clusterId);
    try {
      long deadline = System.currentTimeMillis() + conf.getRawKVCleanTimeoutInMS();
      doSendDeleteRange(backOffer, startKey, endKey, deadline, cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  private List<TiRegion> fetchRegionsFromRange(
      BackOffer backOffer, ByteString startKey, ByteString endKey) {
    List<TiRegion> regions = new ArrayList<>();
    while (startKey.isEmpty()
        || endKey.isEmpty()
        || Key.toRawKey(startKey).compareTo(Key.toRawKey(endKey)) < 0) {
      TiRegion currentRegion = regionManager.getRegionByKey(startKey, backOffer);
      regions.add(currentRegion);
      startKey = currentRegion.getEndKey();
      if (currentRegion.getEndKey().isEmpty()) {
        break;
      }
    }
    return regions;
  }

  private List<DeleteRange> split(BackOffer backOffer, ByteString startKey, ByteString endKey) {
    List<TiRegion> regions = fetchRegionsFromRange(backOffer, startKey, endKey);
    List<DeleteRange> ranges = new ArrayList<>();
    for (int i = 0; i < regions.size(); i++) {
      TiRegion region = regions.get(i);
      ByteString start = i == 0 ? startKey : region.getStartKey();
      ByteString end = i == regions.size() - 1 ? endKey : region.getEndKey();
      ranges.add(new DeleteRange(backOffer, region, start, end));
    }
    return ranges;
  }

  private void doSendDeleteRange(
      BackOffer backOffer, ByteString startKey, ByteString endKey, long deadline, String cf) {
    ExecutorCompletionService<List<DeleteRange>> completionService =
        new ExecutorCompletionService<>(deleteRangeThreadPool);

    List<DeleteRange> ranges = split(backOffer, startKey, endKey);

    List<Future<List<DeleteRange>>> futureList = new ArrayList<>(ranges.size());
    Queue<List<DeleteRange>> taskQueue = new LinkedList<>();
    taskQueue.offer(ranges);
    while (!taskQueue.isEmpty()) {
      List<DeleteRange> task = taskQueue.poll();
      for (DeleteRange range : task) {
        futureList.add(
            completionService.submit(
                () -> doSendDeleteRangeWithRetry(range.getBackOffer(), range, cf)));
      }
      try {
        getTasks(completionService, taskQueue, task, deadline - System.currentTimeMillis());
      } catch (Exception e) {
        for (Future<List<DeleteRange>> future : futureList) {
          future.cancel(true);
        }
        throw e;
      }
    }
  }

  private List<DeleteRange> doSendDeleteRangeWithRetry(
      BackOffer backOffer, DeleteRange range, String cf) {

    while (true) {
      TiStore store = null;
      try {
        Pair<TiStore, StoreClient> storeAndClient = getStoreClient(range, backOffer);
        store = storeAndClient.first;
        storeAndClient.second.rawDeleteRange(
            range.getRegion(), backOffer, range.getStartKey(), range.getEndKey(), cf);
        return EMPTY_DELETERANGE_LIST;
      } catch (NeedRetryException e) {
        ExceptionHandlerUtils.handleNeedRetryException(e, range, range.getRegion(), regionManager);
      } catch (NeedResplitAndRetryException e) {
        ExceptionHandlerUtils.handleNeedResplitAndRetryException(
            e, range, range.getRegion(), backOffer, regionManager);
        return split(backOffer, range.getStartKey(), range.getEndKey());
      } catch (NeedNotRetryException e) {
        ExceptionHandlerUtils.handleNeedNotRetryException(e);
      } catch (final TiKVException e) {
        ExceptionHandlerUtils.handleTiKVException(e, backOffer, regionManager);
      } catch (StatusRuntimeException e) {
        // Rpc error
        ExceptionHandlerUtils.handleStatusRuntimeException(
            e, range, store, range.getRegion(), backOffer, regionManager, storeClientManager);
      }
    }
  }

  @Override
  public void deletePrefix(ByteString key) {
    ByteString endKey = Key.toRawKey(key).nextPrefix().toByteString();
    deleteRange(key, endKey);
  }

  @Override
  public void deletePrefix(ByteString key, String cf) {
    ByteString endKey = Key.toRawKey(key).nextPrefix().toByteString();
    deleteRange(key, endKey, cf);
  }

  @Override
  public TiSession getSession() {
    return tiSession;
  }

  @Override
  public void close() {}

  public synchronized void ingest(List<Pair<ByteString, ByteString>> list) {
    ingest(list, null);
  }

  /**
   * Ingest KV pairs to RawKV using StreamKV API.
   *
   * @param list
   * @param ttl the ttl of the key (in seconds), 0 means the key will never be outdated
   */
  public synchronized void ingest(List<Pair<ByteString, ByteString>> list, Long ttl)
      throws GrpcException {
    if (list.isEmpty()) {
      return;
    }

    Key min = Key.MAX;
    Key max = Key.MIN;
    Map<ByteString, ByteString> map = new HashMap<>(list.size());

    for (Pair<ByteString, ByteString> pair : list) {
      map.put(pair.first, pair.second);
      Key key = Key.toRawKey(pair.first.toByteArray());
      if (key.compareTo(min) < 0) {
        min = key;
      }
      if (key.compareTo(max) > 0) {
        max = key;
      }
    }

    SwitchTiKVModeClient switchTiKVModeClient = tiSession.getSwitchTiKVModeClient();

    try {
      // switch to normal mode
      switchTiKVModeClient.switchTiKVToNormalMode();

      // region split
      List<byte[]> splitKeys = new ArrayList<>(2);
      splitKeys.add(min.getBytes());
      splitKeys.add(max.next().getBytes());
      tiSession.splitRegionAndScatter(splitKeys);
      tiSession.getRegionManager().invalidateAll();

      // switch to import mode
      switchTiKVModeClient.keepTiKVToImportMode();

      // group keys by region
      List<ByteString> keyList = list.stream().map(pair -> pair.first).collect(Collectors.toList());
      Map<TiRegion, List<ByteString>> groupKeys =
          groupKeysByRegion(regionManager, keyList, defaultBackOff());

      // ingest for each region
      for (Map.Entry<TiRegion, List<ByteString>> entry : groupKeys.entrySet()) {
        TiRegion region = entry.getKey();
        List<ByteString> keys = entry.getValue();
        List<Pair<ByteString, ByteString>> kvs =
            keys.stream().map(k -> Pair.create(k, map.get(k))).collect(Collectors.toList());
        doIngest(region, kvs, ttl);
      }
    } finally {
      // swith tikv to normal mode
      switchTiKVModeClient.stopKeepTiKVToImportMode();
      switchTiKVModeClient.switchTiKVToNormalMode();
    }
  }

  private void doIngest(TiRegion region, List<Pair<ByteString, ByteString>> sortedList, Long ttl)
      throws GrpcException {
    if (sortedList.isEmpty()) {
      return;
    }

    ByteString uuid = ByteString.copyFrom(genUUID());
    Key minKey = Key.toRawKey(sortedList.get(0).first);
    Key maxKey = Key.toRawKey(sortedList.get(sortedList.size() - 1).first);
    ImporterClient importerClient =
        new ImporterClient(tiSession, uuid, minKey, maxKey, region, ttl);
    importerClient.write(sortedList.iterator());
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, ByteString endKey, int limit) {
    return scan0(startKey, endKey, limit, false, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param cf column family
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(
      ByteString startKey, ByteString endKey, int limit, String cf) {
    return scan0(startKey, endKey, limit, false, cf);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, int limit) {
    return scan0(startKey, ByteString.EMPTY, limit, false, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param cf column family
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, int limit, String cf) {
    return scan0(startKey, ByteString.EMPTY, limit, false, cf);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, int limit, boolean keyOnly) {
    return scan0(startKey, ByteString.EMPTY, limit, keyOnly, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, ♾)
   *
   * @param startKey raw start key, inclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @param cf column family
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(
      ByteString startKey, int limit, boolean keyOnly, String cf) {
    return scan0(startKey, ByteString.EMPTY, limit, keyOnly, cf);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    return scan0(startKey, endKey, limit, keyOnly, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param limit limit of key-value pairs scanned, should be less than {@link #MAX_RAW_SCAN_LIMIT}
   * @param keyOnly whether to scan in key-only mode
   * @param cf column family
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(
      ByteString startKey, ByteString endKey, int limit, boolean keyOnly, String cf) {
    String[] labels = withClusterId("client_raw_scan");
    Histogram.Timer requestTimer = RAW_REQUEST_LATENCY.labels(labels).startTimer();
    try {
      Iterator<Kvrpcpb.KvPair> iterator =
          rawScanIterator(
              conf, clientBuilder, startKey, endKey, limit, keyOnly, defaultBackOff(), cf);
      RAW_REQUEST_SUCCESS.labels(labels).inc();
      return iterator;
    } catch (Exception e) {
      RAW_REQUEST_FAILURE.labels(labels).inc();
      throw e;
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, ByteString endKey) {
    return scan0(startKey, endKey, false, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param cf column family
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, ByteString endKey, String cf) {
    return scan0(startKey, endKey, false, cf);
  }

  private Iterator<Kvrpcpb.KvPair> scan0(ScanOption scanOption) {
    ByteString startKey = scanOption.getStartKey();
    ByteString endKey = scanOption.getEndKey();
    int limit = scanOption.getLimit();
    boolean keyOnly = scanOption.isKeyOnly();
    return scan0(startKey, endKey, limit, keyOnly);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param limit limit of keys retrieved
   * @param keyOnly whether to scan in keyOnly mode
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<Kvrpcpb.KvPair> scanPrefix0(ByteString prefixKey, int limit, boolean keyOnly) {
    return scan0(
        prefixKey,
        Key.toRawKey(prefixKey).nextPrefix().toByteString(),
        limit,
        keyOnly,
        ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param limit limit of keys retrieved
   * @param keyOnly whether to scan in keyOnly mode
   * @param cf column family
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<Kvrpcpb.KvPair> scanPrefix0(
      ByteString prefixKey, int limit, boolean keyOnly, String cf) {
    return scan0(
        prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), limit, keyOnly, cf);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<Kvrpcpb.KvPair> scanPrefix0(ByteString prefixKey) {
    return scan0(
        prefixKey,
        Key.toRawKey(prefixKey).nextPrefix().toByteString(),
        false,
        ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param cf column family
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<Kvrpcpb.KvPair> scanPrefix0(ByteString prefixKey, String cf) {
    return scan0(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), false, cf);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param keyOnly whether to scan in keyOnly mode
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<Kvrpcpb.KvPair> scanPrefix0(ByteString prefixKey, boolean keyOnly) {
    return scan0(
        prefixKey,
        Key.toRawKey(prefixKey).nextPrefix().toByteString(),
        keyOnly,
        ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan keys with prefix
   *
   * @param prefixKey prefix key
   * @param keyOnly whether to scan in keyOnly mode
   * @param cf column family
   * @return kvPairs iterator with the specified prefix
   */
  public Iterator<Kvrpcpb.KvPair> scanPrefix0(ByteString prefixKey, boolean keyOnly, String cf) {
    return scan0(prefixKey, Key.toRawKey(prefixKey).nextPrefix().toByteString(), keyOnly, cf);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param keyOnly whether to scan in key-only mode
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(ByteString startKey, ByteString endKey, boolean keyOnly) {
    return scan0(startKey, endKey, keyOnly, ConfigUtils.DEF_TIKV_DATA_CF);
  }

  /**
   * Scan all raw key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey raw start key, inclusive
   * @param endKey raw end key, exclusive
   * @param keyOnly whether to scan in key-only mode
   * @param cf column family
   * @return iterator of key-value pairs in range
   */
  public Iterator<Kvrpcpb.KvPair> scan0(
      ByteString startKey, ByteString endKey, boolean keyOnly, String cf) {
    return new TikvIterator(startKey, endKey, keyOnly, cf);
  }

  private BackOffer defaultBackOff() {
    return ConcreteBackOffer.newCustomBackOff(conf.getRawKVDefaultBackoffInMS(), clusterId);
  }

  public class TikvIterator implements Iterator<Kvrpcpb.KvPair> {

    private final ByteString startKey;
    private final ByteString endKey;
    private final boolean keyOnly;
    private final String cf;
    private Iterator<Kvrpcpb.KvPair> iterator;
    private Kvrpcpb.KvPair last;

    public TikvIterator(ByteString startKey, ByteString endKey, boolean keyOnly, String cf) {
      this.startKey = startKey;
      this.endKey = endKey;
      this.keyOnly = keyOnly;
      this.cf = cf;

      this.iterator =
          rawScanIterator(
              conf,
              clientBuilder,
              this.startKey,
              this.endKey,
              conf.getScanBatchSize(),
              keyOnly,
              defaultBackOff(),
              cf);
    }

    @Override
    public boolean hasNext() {
      if (this.iterator.hasNext()) {
        return true;
      }
      if (this.last == null) {
        return false;
      }
      ByteString startKey = Key.toRawKey(this.last.getKey()).next().toByteString();
      this.iterator =
          rawScanIterator(
              conf,
              clientBuilder,
              startKey,
              endKey,
              conf.getScanBatchSize(),
              keyOnly,
              defaultBackOff(),
              cf);
      this.last = null;
      return this.iterator.hasNext();
    }

    @Override
    public Kvrpcpb.KvPair next() {
      Kvrpcpb.KvPair next = this.iterator.next();
      this.last = next;
      return next;
    }
  }

  public Long getClusterId() {
    return clusterId;
  }

  public List<URI> getPdAddresses() {
    return pdAddresses;
  }

  public TiSession getTiSession() {
    return tiSession;
  }

  public RegionStoreClient.RegionStoreClientBuilder getClientBuilder() {
    return clientBuilder;
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public boolean isAtomicForCAS() {
    return atomicForCAS;
  }

  public ExecutorService getBatchGetThreadPool() {
    return batchGetThreadPool;
  }

  public ExecutorService getBatchPutThreadPool() {
    return batchPutThreadPool;
  }

  public ExecutorService getBatchDeleteThreadPool() {
    return batchDeleteThreadPool;
  }

  public ExecutorService getBatchScanThreadPool() {
    return batchScanThreadPool;
  }

  public ExecutorService getDeleteRangeThreadPool() {
    return deleteRangeThreadPool;
  }
}
