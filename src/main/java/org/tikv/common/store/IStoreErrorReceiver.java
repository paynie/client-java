package org.tikv.common.store;

import org.tikv.common.util.BackOffer;

public interface IStoreErrorReceiver {
  boolean onStoreUnreachable(BackOffer backOffer);
}
