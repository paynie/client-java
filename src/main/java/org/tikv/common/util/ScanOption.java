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

package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.ConfigUtils;

/** if Limit of a ScanBatch is 0, it means to scan all keys in */
public class ScanOption {
  private final ByteString startKey;
  private final ByteString endKey;
  private final String cf;
  private final int limit;
  private final boolean keyOnly;

  private ScanOption(ByteString startKey, ByteString endKey, int limit, boolean keyOnly) {
    this(startKey, endKey, ConfigUtils.DEF_TIKV_DATA_CF, limit, keyOnly);
  }

  private ScanOption(
      ByteString startKey, ByteString endKey, String cf, int limit, boolean keyOnly) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.cf = cf;
    this.limit = limit;
    this.keyOnly = keyOnly;
  }

  public static ScanOptionBuilder newBuilder() {
    return new ScanOptionBuilder();
  }

  public ByteString getStartKey() {
    return startKey;
  }

  public ByteString getEndKey() {
    return endKey;
  }

  public int getLimit() {
    return limit;
  }

  public boolean isKeyOnly() {
    return keyOnly;
  }

  public String getCf() {
    return cf;
  }

  public static class ScanOptionBuilder {
    private ByteString startKey;
    private ByteString endKey;
    private String cf;
    private int limit;
    private boolean keyOnly;

    private ScanOptionBuilder() {
      this.startKey = ByteString.EMPTY;
      this.endKey = ByteString.EMPTY;
      this.cf = ConfigUtils.DEF_TIKV_DATA_CF;
      this.limit = 0;
      this.keyOnly = false;
    }

    public ScanOption build() {
      return new ScanOption(startKey, endKey, cf, limit, keyOnly);
    }

    public ScanOptionBuilder setStartKey(ByteString startKey) {
      this.startKey = startKey;
      return this;
    }

    public ScanOptionBuilder setEndKey(ByteString endKey) {
      this.endKey = endKey;
      return this;
    }

    public ScanOptionBuilder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public ScanOptionBuilder setKeyOnly(boolean keyOnly) {
      this.keyOnly = keyOnly;
      return this;
    }

    public ScanOptionBuilder setCf(String cf) {
      this.cf = cf;
      return this;
    }
  }
}
