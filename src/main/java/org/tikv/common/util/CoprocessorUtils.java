package org.tikv.common.util;

import com.google.protobuf.ByteString;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;

public class CoprocessorUtils {
  public static final String COPROCESSOR_PLUGIN_NAME = "graph_meta_plugin";
  public static final String COPROCESSOR_PLUGIN_VERSION = "0.1.4";

  public static ByteString generateCoprocessorParams(CoprocessorRequestType type) {
    CodecDataOutput cdo = new CodecDataOutput(4);
    cdo.writeInt(type.getValue());
    return cdo.toByteString();
  }

  public static long getLongResult(ByteString result) {
    CodecDataInput cdi = new CodecDataInput(result);
    // Parse request type
    cdi.readInt();
    // Parse count
    return cdi.readLong();
  }
}
