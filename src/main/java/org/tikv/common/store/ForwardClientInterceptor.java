package org.tikv.common.store;

import io.grpc.*;
import org.tikv.common.TiConfiguration;
import org.tikv.common.region.TiStore;

public class ForwardClientInterceptor implements ClientInterceptor {
  private final TiStore store;

  public ForwardClientInterceptor(TiStore store) {
    this.store = store;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> methodDescriptor, CallOptions callOptions, Channel channel) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        channel.newCall(methodDescriptor, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        headers.put(TiConfiguration.FORWARD_META_DATA_KEY, store.getStore().getAddress());
        super.start(responseListener, headers);
      }
    };
  }

  public TiStore getStore() {
    return store;
  }
}
