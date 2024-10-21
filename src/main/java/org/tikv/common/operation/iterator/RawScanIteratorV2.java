package org.tikv.common.operation.iterator;

import com.google.protobuf.ByteString;
import io.grpc.StatusRuntimeException;
import org.tikv.common.TiConfiguration;
import org.tikv.common.exception.*;
import org.tikv.common.key.Key;
import org.tikv.common.region.*;
import org.tikv.common.store.StoreClient;
import org.tikv.common.store.StoreClientManager;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ExceptionHandlerUtils;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;

public class RawScanIteratorV2 extends ScanIterator {
  private final BackOffer scanBackOffer;
  private final RegionManager regionManager;
  private final StoreClientManager storeClientManager;

  public RawScanIteratorV2(
      TiConfiguration conf,
      RegionStoreClient.RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      int limit,
      boolean keyOnly,
      BackOffer scanBackOffer,
      String cf,
      RegionManager regionManager,
      StoreClientManager storeClientManager) {
    super(conf, builder, startKey, endKey, limit, keyOnly, cf);

    this.scanBackOffer = scanBackOffer;
    this.regionManager = regionManager;
    this.storeClientManager = storeClientManager;
  }

  @Override
  TiRegion loadCurrentRegionToCache() throws GrpcException {
    BackOffer backOffer = scanBackOffer;
    while (true) {
      TiRegion region = null;
      TiStore store = null;
      try {
        Pair<TiRegion, TiStore> pair =
            regionManager.getRegionStorePairByKey(startKey, TiStoreType.TiKV, backOffer);
        region = pair.first;
        store = pair.second;
        ExceptionHandlerUtils.checkStore(store, region, backOffer, regionManager);
        StoreClient storeClient = storeClientManager.getStoreClient(pair.second);

        if (limit <= 0) {
          currentCache = null;
        } else {
          currentCache =
              storeClient.rawScan(
                  region, backOffer, startKey, endKey.toByteString(), limit, keyOnly, cf);
        }
        return region;
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
  }

  private boolean endOfScan() {
    if (!processingLastBatch) {
      return false;
    }
    ByteString lastKey = currentCache.get(index).getKey();
    return !lastKey.isEmpty() && Key.toRawKey(lastKey).compareTo(endKey) >= 0;
  }

  boolean isCacheDrained() {
    return currentCache == null || limit <= 0 || index >= currentCache.size() || index == -1;
  }

  @Override
  public boolean hasNext() {
    if (isCacheDrained() && cacheLoadFails()) {
      endOfScan = true;
      return false;
    }
    // continue when cache is empty but not null
    while (currentCache != null && currentCache.isEmpty()) {
      if (cacheLoadFails()) {
        return false;
      }
    }
    return !endOfScan();
  }

  private Kvrpcpb.KvPair getCurrent() {
    --limit;
    return currentCache.get(index++);
  }

  @Override
  public Kvrpcpb.KvPair next() {
    return getCurrent();
  }
}
