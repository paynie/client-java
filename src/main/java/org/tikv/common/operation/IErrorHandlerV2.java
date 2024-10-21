package org.tikv.common.operation;

import org.tikv.common.util.BackOffer;

public interface IErrorHandlerV2<RespT> {
  /**
   * Handle the error received in the response after a calling process completes.
   *
   * @param backOffer Back offer used for retry
   * @param resp the response to handle
   */
  void handleResponse(BackOffer backOffer, RespT resp);

  /**
   * Handle the error received during a calling process before it could return a normal response.
   *
   * @param backOffer Back offer used for retry
   * @param e Exception received during a calling process
   */
  void handleRequestError(BackOffer backOffer, Exception e);
}
