/*
 * Copyright 2021 TiKV Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.tikv.common.region;

import static org.tikv.common.codec.KeyUtils.formatBytesUTF8;
import static org.tikv.common.util.KeyRangeUtils.makeRange;

import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.key.Key;
import org.tikv.common.util.BackOffer;

public class RegionCache {
  private static final Logger logger = LoggerFactory.getLogger(RegionCache.class);
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock writeLock = readWriteLock.writeLock();
  private final Lock readLock = readWriteLock.readLock();

  private final Map<Long, TiRegion> regionCache;
  private final Map<Long, TiStore> storeCache;
  private final RangeMap<Key, Long> keyToRegionIdCache;

  public RegionCache() {
    regionCache = new HashMap<>();
    storeCache = new HashMap<>();

    keyToRegionIdCache = TreeRangeMap.create();
  }

  public void invalidateAll() {
    writeLock.lock();
    try {
      regionCache.clear();
      storeCache.clear();
      keyToRegionIdCache.clear();
    } finally {
      writeLock.unlock();
    }
  }

  public TiRegion getRegionByKey(ByteString key, BackOffer backOffer) {
    readLock.lock();
    try {
      Long regionId;
      if (key.isEmpty()) {
        // if key is empty, it must be the start key.
        regionId = keyToRegionIdCache.get(Key.toRawKey(key, true));
      } else {
        regionId = keyToRegionIdCache.get(Key.toRawKey(key));
      }
      if (logger.isDebugEnabled()) {
        logger.debug(
            String.format("getRegionByKey key[%s] -> ID[%s]", formatBytesUTF8(key), regionId));
      }

      if (regionId == null) {
        return null;
      }
      TiRegion region;
      region = regionCache.get(regionId);
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("getRegionByKey ID[%s] -> Region[%s]", regionId, region));
      }
      return region;
    } finally {
      readLock.unlock();
    }
  }

  public TiRegion putRegion(TiRegion region) {
    writeLock.lock();
    try {
      if (logger.isDebugEnabled()) {
        logger.debug("putRegion: " + region);
      }
      TiRegion oldRegion = regionCache.get(region.getId());
      if (oldRegion != null) {
        if (oldRegion.getMeta().equals(region.getMeta())) {
          return oldRegion;
        } else {
          invalidateRegion(oldRegion);
        }
      }
      regionCache.put(region.getId(), region);
      keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
      return region;
    } finally {
      writeLock.unlock();
    }
  }

  @Deprecated
  public TiRegion getRegionById(long regionId) {
    readLock.lock();
    try {
      TiRegion region = regionCache.get(regionId);
      if (logger.isDebugEnabled()) {
        logger.debug(String.format("getRegionByKey ID[%s] -> Region[%s]", regionId, region));
      }
      return region;
    } finally {
      readLock.unlock();
    }
  }

  private TiRegion getRegionFromCache(long regionId) {
    readLock.lock();
    try {
      return regionCache.get(regionId);
    } finally {
      readLock.unlock();
    }
  }

  /** Removes region associated with regionId from regionCache. */
  public void invalidateRegion(TiRegion region) {
    writeLock.lock();
    try {
      try {
        TiRegion oldRegion = regionCache.get(region.getId());
        if (oldRegion != null && oldRegion.getMeta().equals(region.getMeta())) {
          keyToRegionIdCache.remove(makeRange(region.getStartKey(), region.getEndKey()));
          regionCache.remove(region.getId());
          oldRegion.needUpdate();
          logger.info(
              String.format(
                  "Invalid region {id: %d, leader: %d} success",
                  region.getId(), region.getLeader().getStoreId()));
        } else {
          if (oldRegion == null) {
            logger.warn(
                String.format(
                    "Invalid region {id: %d, leader: %d} failed, can not find region ",
                    region.getId(), region.getLeader().getStoreId()));
          } else {
            logger.warn(
                String.format(
                    "Invalid region {id: %d, leader: %d} failed, "
                        + "old region is {id: %d, leader: %d}",
                    region.getId(),
                    region.getLeader().getStoreId(),
                    oldRegion.getId(),
                    oldRegion.getLeader().getStoreId()));
          }
        }
      } catch (Exception ignore) {
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void insertRegionToCache(TiRegion region) {
    writeLock.lock();
    try {
      try {
        TiRegion oldRegion = regionCache.get(region.getId());
        if (oldRegion != null) {
          keyToRegionIdCache.remove(makeRange(oldRegion.getStartKey(), oldRegion.getEndKey()));
        }
        regionCache.put(region.getId(), region);
        keyToRegionIdCache.put(makeRange(region.getStartKey(), region.getEndKey()), region.getId());
      } catch (Exception ignore) {
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean updateRegion(TiRegion expected, TiRegion region) {
    writeLock.lock();
    try {
      try {
        if (logger.isDebugEnabled()) {
          logger.debug(String.format("invalidateRegion ID[%s]", region.getId()));
        }
        TiRegion oldRegion = regionCache.get(region.getId());

        // TODO: region
        if (oldRegion != null && !expected.getMeta().equals(oldRegion.getMeta())) {
          return false;
        } else {
          if (oldRegion != null) {
            keyToRegionIdCache.remove(makeRange(oldRegion.getStartKey(), oldRegion.getEndKey()));
          }
          regionCache.put(region.getId(), region);
          keyToRegionIdCache.put(
              makeRange(region.getStartKey(), region.getEndKey()), region.getId());
          return true;
        }
      } catch (Exception e) {
        logger.warn("Update region failed, ", e);
        return false;
      }
    } finally {
      writeLock.unlock();
    }
  }

  public boolean updateStore(TiStore oldStore, TiStore newStore) {
    writeLock.lock();
    try {
      if (!newStore.isValid()) {
        return false;
      }
      if (oldStore == null) {
        storeCache.put(newStore.getId(), newStore);
        return true;
      }
      TiStore originStore = storeCache.get(oldStore.getId());
      if (originStore.equals(oldStore)) {
        storeCache.put(newStore.getId(), newStore);
        oldStore.markInvalid();
        return true;
      }
      return false;
    } finally {
      writeLock.unlock();
    }
  }

  public void invalidateAllRegionForStore(TiStore store) {
    writeLock.lock();
    try {
      TiStore oldStore = storeCache.get(store.getId());
      if (oldStore != store) {
        return;
      }
      List<TiRegion> regionToRemove = new ArrayList<>();
      for (TiRegion r : regionCache.values()) {
        if (r.getLeader().getStoreId() == store.getId()) {
          if (logger.isDebugEnabled()) {
            logger.debug(String.format("invalidateAllRegionForStore Region[%s]", r));
          }
          regionToRemove.add(r);
        }
      }

      logger.warn(String.format("invalid store [%d]", store.getId()));
      // remove region
      for (TiRegion r : regionToRemove) {
        keyToRegionIdCache.remove(makeRange(r.getStartKey(), r.getEndKey()));
        regionCache.remove(r.getId());
      }
    } finally {
      writeLock.unlock();
    }
  }

  public void invalidateStore(long storeId) {
    writeLock.lock();
    try {
      TiStore store = storeCache.remove(storeId);
      if (store != null) {
        store.markInvalid();
      }
    } finally {
      writeLock.unlock();
    }
  }

  public TiStore getStoreById(long id) {
    readLock.lock();
    try {
      return storeCache.get(id);
    } finally {
      readLock.unlock();
    }
  }

  public boolean putStore(long id, TiStore store) {
    writeLock.lock();
    try {
      TiStore oldStore = storeCache.get(id);
      if (oldStore != null) {
        if (oldStore.equals(store)) {
          return false;
        } else {
          oldStore.markInvalid();
        }
      }
      storeCache.put(id, store);
      return true;
    } finally {
      writeLock.unlock();
    }
  }

  public void clearAll() {
    writeLock.lock();
    try {
      keyToRegionIdCache.clear();
      regionCache.clear();
    } finally {
      writeLock.unlock();
    }
  }

  public List<TiStore> getAllStores() {
    readLock.lock();
    try {
      List<TiStore> stores = new ArrayList<>(storeCache.size());
      stores.addAll(storeCache.values());
      return stores;
    } finally {
      readLock.unlock();
    }
  }
}
