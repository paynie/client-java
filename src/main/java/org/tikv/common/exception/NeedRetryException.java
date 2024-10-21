package org.tikv.common.exception;

public class NeedRetryException extends TiKVException {
  public NeedRetryException(Throwable e) {
    super(e);
  }

  public NeedRetryException(String msg) {
    super(msg);
  }

  public NeedRetryException(String msg, Throwable e) {
    super(msg, e);
  }
}
