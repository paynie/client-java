package org.tikv.common.store;

import org.tikv.common.region.IRegionErrorReceiver;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.BackOffer;

public class ErrorReceiver implements IStoreErrorReceiver, IRegionErrorReceiver {
  @Override
  public boolean onStoreUnreachable(BackOffer backOffer) {
    return false;
  }

  @Override
  public boolean onNotLeader(TiRegion region, BackOffer backOffer) {
    return false;
  }
}
