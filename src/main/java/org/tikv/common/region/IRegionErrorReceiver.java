package org.tikv.common.region;

import org.tikv.common.util.BackOffer;

public interface IRegionErrorReceiver {
  boolean onNotLeader(TiRegion region, BackOffer backOffer);
}
