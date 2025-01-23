package org.tikv.common.store;

import io.grpc.Channel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.PDClient;
import org.tikv.common.TiConfiguration;
import org.tikv.common.apiversion.RequestKeyCodec;
import org.tikv.common.region.RegionManager;
import org.tikv.common.region.TiStore;
import org.tikv.common.region.TiStoreType;
import org.tikv.common.util.ChannelFactory;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.kvproto.Metapb;
import org.tikv.kvproto.TikvGrpc;

/** Store client manager */
public class StoreClientManager {
  public static final int HEARTBEAT_TIMEOUT_MS = 60000;
  public static final int MAX_CHECK_STORE_TOMBSTONE_TICK = 60;
  public static final int LOCK_SLOT_NUMBER = 16;
  private static final Logger logger = LoggerFactory.getLogger(StoreClientManager.class);
  /** Store id to client map */
  private final Map<Long, StoreClient> storeIdToClientMap = new HashMap<>();
  /** TiKV cluster id */
  private final long clusterId;
  /** Configuration */
  private final TiConfiguration conf;
  /** RPC channel factory */
  private final ChannelFactory channelFactory;
  /** Region manager */
  private final RegionManager regionManager;
  /** Key codec method */
  private final RequestKeyCodec codec;
  /** PD client */
  private final PDClient pdClient;
  /** The executor service for the store health check */
  private final ScheduledExecutorService executor;
  /** Locks */
  private final ReentrantLock[] locks;

  public StoreClientManager(
      long clusterId,
      TiConfiguration conf,
      ChannelFactory channelFactory,
      RegionManager regionManager,
      RequestKeyCodec codec,
      PDClient pdClient) {
    this.clusterId = clusterId;
    this.conf = conf;
    this.channelFactory = channelFactory;
    this.regionManager = regionManager;
    this.codec = codec;
    this.pdClient = pdClient;

    locks = new ReentrantLock[LOCK_SLOT_NUMBER];
    for (int i = 0; i < LOCK_SLOT_NUMBER; i++) {
      locks[i] = new ReentrantLock();
    }

    this.executor = Executors.newScheduledThreadPool(1);
    this.executor.scheduleAtFixedRate(new StoreHealthyCheckV2(), 1000, 1000, TimeUnit.MILLISECONDS);
  }

  private ReentrantLock getLock(TiStore store) {
    return locks[(int) (store.getId() % LOCK_SLOT_NUMBER)];
  }

  public StoreClient getStoreClient(TiStore store) {
    if (store == null || pdClient.getHostMapping() == null) {
      throw new IllegalArgumentException(
          "Invalid argument!, store and hostMapping can not be null");
    }

    ReentrantLock lock = getLock(store);
    lock.lock();
    try {
      StoreClient storeClient = storeIdToClientMap.get(store.getId());
      if (storeClient == null) {
        // Init
        String addressStr = store.getStore().getAddress();
        logger.info(String.format("Create region store client on address %s", addressStr));
        Channel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
        TikvGrpc.TikvBlockingStub tikvBlockingStub = TikvGrpc.newBlockingStub(channel);
        TikvGrpc.TikvFutureStub tikvAsyncStub = TikvGrpc.newFutureStub(channel);

        storeClient =
            new StoreClient(
                clusterId,
                conf,
                channelFactory,
                regionManager,
                codec,
                TiStoreType.TiKV,
                store,
                tikvBlockingStub,
                tikvAsyncStub);
        storeIdToClientMap.put(store.getId(), storeClient);
      }
      return storeClient;
    } finally {
      lock.unlock();
    }
  }

  public void remove(TiStore store) {
    ReentrantLock lock = getLock(store);
    lock.lock();
    try {
      StoreClient client = storeIdToClientMap.remove(store.getId());
      client.close();
    } catch (Exception e) {
      // Ignore
    } finally {
      lock.unlock();
    }
  }

  public TiConfiguration getConf() {
    return conf;
  }

  public ChannelFactory getChannelFactory() {
    return channelFactory;
  }

  public RegionManager getRegionManager() {
    return regionManager;
  }

  public RequestKeyCodec getCodec() {
    return codec;
  }

  public void storeUnreachable(TiStore store) {
    if (store.getReachable().compareAndSet(true, false)) {
      logger.error(
          String.format(
              "Store {id: %d, address: %s} not reachable", store.getId(), store.getAddress()));

      // Close all channels
      remove(store);
      channelFactory.closeChannel(store.getAddress());
    }
  }

  public void storeReachable(TiStore store) {
    if (store.getReachable().compareAndSet(false, true)) {
      logger.info(
          String.format(
              "Store {id: %d, address: %s} recover to reachable",
              store.getId(), store.getAddress()));
    }
  }

  public void close() {
    if (executor != null) {
      executor.shutdownNow();
    }
  }

  class StoreHealthyCheckV2 implements Runnable {

    private long checkTombstoneTick;

    @Override
    public void run() {
      checkTombstoneTick += 1;
      boolean needCheckTombstoneStore = false;
      if (checkTombstoneTick >= MAX_CHECK_STORE_TOMBSTONE_TICK) {
        needCheckTombstoneStore = true;
        checkTombstoneTick = 0;
      }

      // Get all store
      List<TiStore> stores = regionManager.getAllStores();
      for (TiStore store : stores) {
        if (!store.isValid()) {
          // Skip invalid store
          continue;
        }

        // Check store is tombstone
        if (needCheckTombstoneStore) {
          if (checkStoreTombstone(store)) {
            continue;
          }
        }

        if (checkStoreHealth(store)) {
          // Health, change the state if need
          storeReachable(store);
        } else {
          // Not health, change the state if need
          storeUnreachable(store);
        }
      }
    }

    private boolean checkStoreHealth(TiStore store) {
      String addressStr = store.getStore().getAddress();
      try {
        Channel channel = channelFactory.getChannel(addressStr, pdClient.getHostMapping());
        HealthGrpc.HealthBlockingStub stub =
            HealthGrpc.newBlockingStub(channel)
                .withDeadlineAfter(HEARTBEAT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        HealthCheckRequest req = HealthCheckRequest.newBuilder().build();
        HealthCheckResponse resp = stub.check(req);
        logger.info("checkStoreHealth for store " + addressStr + " response = " + resp);
        return resp.getStatus() == HealthCheckResponse.ServingStatus.SERVING;
      } catch (Exception e) {
        logger.error("checkStoreHealth for store " + addressStr + " failed, ", e);
        return false;
      }
    }

    private boolean checkStoreTombstone(TiStore store) {
      try {
        Metapb.Store newStore =
            pdClient.getStore(
                ConcreteBackOffer.newRawKVBackOff(pdClient.getClusterId()), store.getId());
        if (newStore != null && newStore.getState() == Metapb.StoreState.Tombstone) {
          return true;
        }
      } catch (Exception e) {
        logger.info("fail to check tombstone stores", e);
        return false;
      }
      return false;
    }
  }
}
