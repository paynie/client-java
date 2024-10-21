package org.tikv.raw;

import com.google.protobuf.ByteString;
import java.util.Random;
import org.tikv.common.codec.Codec;
import org.tikv.common.codec.CodecDataInput;
import org.tikv.common.codec.CodecDataOutput;

public class EncodeUtils {
  public static byte[] getKey(int graphId, int typeId, Random r) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeInt(graphId);
    cdo.writeInt(typeId);
    // cdo.writeLong(r.nextLong());
    // cdo.writeLong(index.getAndIncrement());
    cdo.writeLong(r.nextLong());
    byte[] keyEnd = new byte[64];
    r.nextBytes(keyEnd);
    // cdo.write(keyEnd);

    return cdo.toBytes();
  }

  public static byte[] getKey(int graphId, int typeId, Random r, int keyLen) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeInt(graphId);
    cdo.writeInt(typeId);
    cdo.writeLong(r.nextLong());

    if (keyLen > 16) {
      byte[] keyEnd = new byte[keyLen - 16];
      r.nextBytes(keyEnd);
    }

    return cdo.toBytes();
  }

  public static ByteString decodeKey(ByteString key) {
    return ByteString.copyFrom(decodeKey(key.toByteArray()));
  }

  public static byte[] decodeKey(byte[] key) {
    return Codec.BytesCodec.readBytes(new CodecDataInput(key));
  }

  public static byte[] encodeKey(byte[] key) {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.BytesCodec.writeBytes(cdo, key);
    return cdo.toBytes();
  }

  public static ByteString encodeKey(ByteString key) {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.BytesCodec.writeBytes(cdo, key.toByteArray());
    return cdo.toByteString();
  }

  public static byte[] getStartKey(int graphId, int typeId) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeInt(graphId);
    cdo.writeInt(typeId);
    return cdo.toBytes();
  }

  public static byte[] getEndKey(int graphId, int typeId) {
    CodecDataOutput cdo = new CodecDataOutput();
    cdo.writeInt(graphId);
    cdo.writeInt(typeId + 1);
    return cdo.toBytes();
  }
}
