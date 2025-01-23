package org.tikv.common.store;

import com.google.protobuf.ByteString;
import io.prometheus.client.Histogram;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.tikv.common.AbstractGRPCClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.exception.NeedRetryException;
import org.tikv.common.exception.RawCASConflictException;
import org.tikv.common.log.SlowLog;
import org.tikv.common.operation.RegionErrorHandlerV2;
import org.tikv.common.region.*;
import org.tikv.common.util.*;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.kvproto.Kvrpcpb.RawPutRequest;
import org.tikv.kvproto.Kvrpcpb.RawPutResponse;
import org.tikv.kvproto.TikvGrpc;
import org.tikv.kvproto.Tracepb;

/** The Store client */
public class StoreClient
    extends AbstractGRPCClient<TikvGrpc.TikvBlockingStub, TikvGrpc.TikvFutureStub> {
  public static final Histogram GRPC_RAW_REQUEST_LATENCY =
      HistogramUtils.buildDuration()
          .name("client_java_grpc_raw_requests_latency_v2")
          .help("grpc raw request latency.")
          .labelNames("type", "cluster")
          .register();
  /** TiKV cluster id */
  private final long clusterId;
  /** TiKV cluster id str */
  private final String clusterIdStr;
  /** TiKV region manager */
  private final RegionManager regionManager;
  /** TiKV key codec */
  private final RequestKeyCodec codec;
  /** TiKV store */
  private final TiStore store;
  /** TiKV store type */
  private final TiStoreType storeType;

  protected StoreClient(
      long clusterId,
      TiConfiguration conf,
      ChannelFactory channelFactory,
      RegionManager regionManager,
      RequestKeyCodec codec,
      TiStoreType storeType,
      TiStore store,
      TikvGrpc.TikvBlockingStub tikvBlockingStub,
      TikvGrpc.TikvFutureStub tikvAsyncStub) {
    super(conf, channelFactory, tikvBlockingStub, tikvAsyncStub);
    this.clusterId = clusterId;
    this.clusterIdStr = Long.toString(clusterId);
    this.regionManager = regionManager;
    this.codec = codec;
    this.storeType = storeType;
    this.store = store;

    String addressStr = store.getStore().getAddress();
    logger.info(String.format("Create region store client on address %s", addressStr));
  }

  @Override
  protected TikvGrpc.TikvBlockingStub getBlockingStub() {
    return blockingStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  protected TikvGrpc.TikvFutureStub getAsyncStub() {
    return asyncStub.withDeadlineAfter(getTimeout(), TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws Exception {}

  protected Kvrpcpb.Context makeContext(
      TiRegion region, Set<Long> resolvedLocks, TiStoreType storeType, SlowLog slowLog) {
    Kvrpcpb.Context context = region.getReplicaContext(resolvedLocks, storeType);
    return addTraceId(context, slowLog);
  }

  protected Kvrpcpb.Context makeContext(TiRegion region, TiStoreType storeType, SlowLog slowLog) {
    Kvrpcpb.Context context = region.getReplicaContext(java.util.Collections.emptySet(), storeType);
    return addTraceId(context, slowLog);
  }

  private Kvrpcpb.Context addTraceId(Kvrpcpb.Context context, SlowLog slowLog) {
    if (slowLog.getThresholdMS() < 0) {
      // disable tikv tracing
      return context;
    }
    long traceId = slowLog.getTraceId();
    return Kvrpcpb.Context.newBuilder(context)
        .setTraceContext(
            Tracepb.TraceContext.newBuilder()
                .setDurationThresholdMs(
                    (int) (slowLog.getThresholdMS() * conf.getRawKVServerSlowLogFactor()))
                .addRemoteParentSpans(Tracepb.RemoteParentSpan.newBuilder().setTraceId(traceId)))
        .build();
  }

  /**
   * Write the key value pair to the store
   *
   * @param region the region contain the key
   * @param backOffer retry backoff
   * @param key the key
   * @param value the value
   * @param ttl the time to live of the key
   * @param atomicForCAS use atomic for cas or not
   * @param cf column family
   */
  public void rawPut(
      TiRegion region,
      BackOffer backOffer,
      ByteString key,
      ByteString value,
      long ttl,
      boolean atomicForCAS,
      String cf) {

    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_put", Long.toString(clusterId))
            .startTimer();
    try {
      Supplier<RawPutRequest> factory =
          () ->
              RawPutRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setValue(value)
                  .setTtl(ttl)
                  .setForCas(atomicForCAS)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<RawPutResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);
      RawPutResponse resp = call(backOffer, TikvGrpc.getRawPutMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Write the batch of keys/values to the store
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param batch the batch of keys/values
   * @param ttl the time to live of the keys
   * @param atomicForCAS use atomic for cas or not
   * @param cf column family
   */
  public void rawBatchPut(
      TiRegion region,
      BackOffer backOffer,
      Batch batch,
      long ttl,
      boolean atomicForCAS,
      String cf) {
    List<Kvrpcpb.KvPair> pairs = new ArrayList<>(batch.getKeys().size());
    for (int i = 0; i < batch.getKeys().size(); i++) {
      pairs.add(
          Kvrpcpb.KvPair.newBuilder()
              .setKey(codec.encodeKey(batch.getKeys().get(i)))
              .setValue(batch.getValues().get(i))
              .build());
    }
    rawBatchPut(region, backOffer, pairs, ttl, atomicForCAS, cf);
  }

  /**
   * Write the batch of keys/values to the store
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param kvPairs the batch of keys/values
   * @param ttl the time to live of the keys
   * @param atomicForCAS use atomic for cas or not
   * @param cf column family
   */
  public void rawBatchPut(
      TiRegion region,
      BackOffer backOffer,
      List<Kvrpcpb.KvPair> kvPairs,
      long ttl,
      boolean atomicForCAS,
      String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_put", Long.toString(clusterId))
            .startTimer();
    try {
      if (kvPairs.isEmpty()) {
        return;
      }
      Supplier<Kvrpcpb.RawBatchPutRequest> factory =
          () ->
              Kvrpcpb.RawBatchPutRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .addAllPairs(kvPairs)
                  .setTtl(ttl)
                  .addTtls(ttl)
                  .setForCas(atomicForCAS)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawBatchPutResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawBatchPutResponse resp = call(backOffer, TikvGrpc.getRawBatchPutMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Write the batch of write operations to the store
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param writeOps the batch of write operations
   * @param ttl the time to live of the keys
   * @param atomicForCAS use atomic for cas or not
   * @param cf column family
   */
  public void rawBatchWrite(
      TiRegion region,
      BackOffer backOffer,
      List<Kvrpcpb.WriteOp> writeOps,
      long ttl,
      boolean atomicForCAS,
      String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_write", Long.toString(clusterId))
            .startTimer();
    try {
      if (writeOps.isEmpty()) {
        return;
      }
      Supplier<Kvrpcpb.RawBatchWriteRequest> factory =
          () ->
              Kvrpcpb.RawBatchWriteRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .addAllBatch(writeOps)
                  .setTtl(ttl)
                  .addTtls(ttl)
                  .setForCas(atomicForCAS)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawBatchWriteResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawBatchWriteResponse resp =
          call(backOffer, TikvGrpc.getRawBatchWriteMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Write the batch of write operations to the store
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param batch the batch of write operations
   * @param ttl the time to live of the keys
   * @param atomicForCAS use atomic for cas or not
   * @param cf column family
   */
  public void rawBatchWrite(
      TiRegion region,
      BackOffer backOffer,
      WriteBatch batch,
      long ttl,
      boolean atomicForCAS,
      String cf) {
    rawBatchWrite(region, backOffer, batch.getWriteOps(), ttl, atomicForCAS, cf);
  }

  /**
   * Get the value of the key from the store
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param key the key
   * @param cf column family
   * @return value
   */
  public Optional<ByteString> rawGet(
      TiRegion region, ConcreteBackOffer backOffer, ByteString key, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_get", Long.toString(clusterId))
            .startTimer();
    try {
      Supplier<Kvrpcpb.RawGetRequest> factory =
          () ->
              Kvrpcpb.RawGetRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawGetResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawGetResponse resp = call(backOffer, TikvGrpc.getRawGetMethod(), factory);

      // Check error message
      handler.handleResponse(backOffer, resp);

      // Generate result
      if (resp.getNotFound()) {
        return Optional.empty();
      } else {
        return Optional.of(resp.getValue());
      }
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Get the values of the batch keys from the store
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param keys the batch key
   * @param cf column family
   * @return the key/value pairs
   */
  public List<Kvrpcpb.KvPair> rawBatchGet(
      TiRegion region, BackOffer backOffer, List<ByteString> keys, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_get", Long.toString(clusterId))
            .startTimer();
    try {
      if (keys.isEmpty()) {
        return new ArrayList<>();
      }
      Supplier<Kvrpcpb.RawBatchGetRequest> factory =
          () ->
              Kvrpcpb.RawBatchGetRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .addAllKeys(codec.encodeKeys(keys))
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawBatchGetResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawBatchGetResponse resp = call(backOffer, TikvGrpc.getRawBatchGetMethod(), factory);
      handler.handleResponse(backOffer, resp);
      return codec.decodeKvPairs(resp.getPairsList());
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Delete the batch keys/values
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param keys the batch keys need delete
   * @param atomicForCAS use atomic for cas or not
   * @param cf column family
   */
  public void rawBatchDelete(
      TiRegion region,
      BackOffer backOffer,
      List<ByteString> keys,
      boolean atomicForCAS,
      String cf) {

    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY
            .labels("client_grpc_raw_batch_delete", Long.toString(clusterId))
            .startTimer();
    try {
      if (keys.isEmpty()) {
        return;
      }
      Supplier<Kvrpcpb.RawBatchDeleteRequest> factory =
          () ->
              Kvrpcpb.RawBatchDeleteRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .addAllKeys(codec.encodeKeys(keys))
                  .setForCas(atomicForCAS)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawBatchDeleteResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawBatchDeleteResponse resp =
          call(backOffer, TikvGrpc.getRawBatchDeleteMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Get the ttl of the key
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param key the key
   * @param cf column family
   * @return the time to live value of the key
   */
  public Optional<Long> rawGetKeyTTL(
      TiRegion region, ConcreteBackOffer backOffer, ByteString key, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_get_key_ttl", clusterIdStr).startTimer();
    try {
      Supplier<Kvrpcpb.RawGetKeyTTLRequest> factory =
          () ->
              Kvrpcpb.RawGetKeyTTLRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawGetKeyTTLResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawGetKeyTTLResponse resp =
          call(backOffer, TikvGrpc.getRawGetKeyTTLMethod(), factory);
      handler.handleResponse(backOffer, resp);

      if (resp.getNotFound()) {
        return Optional.empty();
      }
      return Optional.of(resp.getTtl());
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Set the time to live of the key
   *
   * @param region the region contain the keys
   * @param backOffer retry backoff
   * @param key the key
   * @param ttl time to live value in seconds
   * @param cf column family
   */
  public void rawSetKeyTTL(
      TiRegion region, ConcreteBackOffer backOffer, ByteString key, long ttl, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_set_key_ttl", clusterIdStr).startTimer();
    try {
      Supplier<Kvrpcpb.RawSetKeyTTLRequest> factory =
          () ->
              Kvrpcpb.RawSetKeyTTLRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setTtl(ttl)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawSetKeyTTLResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawSetKeyTTLResponse resp =
          call(backOffer, TikvGrpc.getRawSetKeyTTLMethod(), factory);

      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Return a batch KvPair list containing limited key-value pairs starting from `key`, which are in
   * the same region
   *
   * @param region the region contain the keys
   * @param backOffer BackOffer
   * @param start startKey
   * @param end endKey
   * @param keyOnly true if value of KvPair is not needed
   * @return KvPair list
   */
  public List<Kvrpcpb.KvPair> rawScan(
      final TiRegion region,
      BackOffer backOffer,
      ByteString start,
      ByteString end,
      int limit,
      boolean keyOnly,
      String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_scan", clusterIdStr).startTimer();
    try {
      Supplier<Kvrpcpb.RawScanRequest> factory =
          () -> {
            ByteString rangeEnd = end;
            if (end == null) {
              rangeEnd = ByteString.EMPTY;
            }

            Pair<ByteString, ByteString> range = codec.encodeRange(start, rangeEnd);
            return Kvrpcpb.RawScanRequest.newBuilder()
                .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                .setCf(cf)
                .setStartKey(range.first)
                .setEndKey(range.second)
                .setKeyOnly(keyOnly)
                .setLimit(limit)
                .build();
          };

      RegionErrorHandlerV2<Kvrpcpb.RawScanResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawScanResponse resp = call(backOffer, TikvGrpc.getRawScanMethod(), factory);
      handler.handleResponse(backOffer, resp);
      return codec.decodeKvPairs(resp.getKvsList());
    } finally {
      requestTimer.observeDuration();
    }
  }

  public void rawDelete(
      TiRegion region, BackOffer backOffer, ByteString key, boolean atomicForCAS, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_delete", clusterIdStr).startTimer();
    try {
      Supplier<Kvrpcpb.RawDeleteRequest> factory =
          () ->
              Kvrpcpb.RawDeleteRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setForCas(atomicForCAS)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawDeleteResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawDeleteResponse resp = call(backOffer, TikvGrpc.getRawDeleteMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Delete the range
   *
   * @param region the region contain the range
   * @param backOffer retry backoff
   * @param startKey the start key of the range (include)
   * @param endKey the start key of the range (exclude)
   * @param cf column family
   */
  public void rawDeleteRange(
      TiRegion region, BackOffer backOffer, ByteString startKey, ByteString endKey, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_delete_range", clusterIdStr).startTimer();
    try {
      Supplier<Kvrpcpb.RawDeleteRangeRequest> factory =
          () -> {
            Pair<ByteString, ByteString> range = codec.encodeRange(startKey, endKey);
            return Kvrpcpb.RawDeleteRangeRequest.newBuilder()
                .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                .setStartKey(range.first)
                .setEndKey(range.second)
                .setCf(cf)
                .build();
          };

      RegionErrorHandlerV2<Kvrpcpb.RawDeleteRangeResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawDeleteRangeResponse resp =
          call(backOffer, TikvGrpc.getRawDeleteRangeMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  /**
   * Compare and set
   *
   * @param region the region contain the key
   * @param backOffer retry backoff
   * @param key the key need update
   * @param prevValue pre value
   * @param value new value
   * @param ttl time to live in seconds
   * @param cf column family
   * @throws RawCASConflictException cas failed
   */
  public void rawCompareAndSet(
      TiRegion region,
      BackOffer backOffer,
      ByteString key,
      Optional<ByteString> prevValue,
      ByteString value,
      long ttl,
      String cf)
      throws RawCASConflictException {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_put_if_absent", clusterIdStr).startTimer();
    try {
      Supplier<Kvrpcpb.RawCASRequest> factory =
          () ->
              Kvrpcpb.RawCASRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setKey(codec.encodeKey(key))
                  .setValue(value)
                  .setPreviousValue(prevValue.orElse(ByteString.EMPTY))
                  .setPreviousNotExist(!prevValue.isPresent())
                  .setTtl(ttl)
                  .setCf(cf)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawCASResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawCASResponse resp = call(backOffer, TikvGrpc.getRawCompareAndSwapMethod(), factory);
      handler.handleResponse(backOffer, resp);
    } finally {
      requestTimer.observeDuration();
    }
  }

  public ByteString rawCoprocessor(
      TiRegion region,
      BackOffer backOffer,
      String coporName,
      String version,
      ByteString serializedParams,
      List<Kvrpcpb.KeyRange> ranges) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_coprocessor", clusterIdStr).startTimer();

    try {
      Supplier<Kvrpcpb.RawCoprocessorRequest> factory =
          () ->
              Kvrpcpb.RawCoprocessorRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setCoprName(coporName)
                  .setCoprVersionReq(version)
                  .setData(serializedParams)
                  .addAllRanges(ranges)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawCoprocessorResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawCoprocessorResponse resp =
          call(backOffer, TikvGrpc.getRawCoprocessorMethod(), factory);
      handler.handleResponse(backOffer, resp);
      return resp.getData();
    } finally {
      requestTimer.observeDuration();
    }
  }

  public long rawCount(
      TiRegion region, BackOffer backOffer, List<Kvrpcpb.KeyRange> ranges, String cf) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_count", clusterIdStr).startTimer();

    try {
      Supplier<Kvrpcpb.RawCountRequest> factory =
          () ->
              Kvrpcpb.RawCountRequest.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .addAllRanges(ranges)
                  .setCf(cf)
                  .setEachLimit(10000)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawCountResponse> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawCountResponse resp = call(backOffer, TikvGrpc.getRawCountMethod(), factory);
      handler.handleResponse(backOffer, resp);
      return resp.getCount();
    } finally {
      requestTimer.observeDuration();
    }
  }

  public Pair<Long, ByteString> rawCountV2(
      TiRegion region, BackOffer backOffer, Kvrpcpb.KeyRange range, String cf, int limit) {
    Histogram.Timer requestTimer =
        GRPC_RAW_REQUEST_LATENCY.labels("client_grpc_raw_count", clusterIdStr).startTimer();

    try {
      Supplier<Kvrpcpb.RawCountRequestV2> factory =
          () ->
              Kvrpcpb.RawCountRequestV2.newBuilder()
                  .setContext(makeContext(region, storeType, backOffer.getSlowLog()))
                  .setRange(range)
                  .setCf(cf)
                  .setEachLimit(limit)
                  .build();

      RegionErrorHandlerV2<Kvrpcpb.RawCountResponseV2> handler =
          new RegionErrorHandlerV2<>(
              regionManager,
              new RegionErrorReceiverV2(region),
              resp -> resp.hasRegionError() ? resp.getRegionError() : null);

      Kvrpcpb.RawCountResponseV2 resp = call(backOffer, TikvGrpc.getRawCountV2Method(), factory);
      handler.handleResponse(backOffer, resp);

      if (!resp.getError().isEmpty()) {
        throw new NeedRetryException(resp.getError());
      }

      return Pair.create(resp.getCount(), resp.getLastKey());
    } finally {
      requestTimer.observeDuration();
    }
  }

  public RegionManager getRegionManager() {
    return regionManager;
  }

  public RequestKeyCodec getCodec() {
    return codec;
  }

  public TiStore getStore() {
    return store;
  }

  public TiStoreType getStoreType() {
    return storeType;
  }
}
