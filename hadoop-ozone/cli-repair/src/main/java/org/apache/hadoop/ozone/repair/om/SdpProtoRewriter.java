/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.ozone.repair.om;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Wire-level protobuf rewriter for SDP-1.4 OM DB values (SDP proto numbers moved to the 1000+ range on the
 * Apache 2.x based SDP line). Works on the encoded bytes, so it does not depend on the generated classes.
 *
 * <p>Field renumbering is guarded by wire type: upstream 2.x reuses some of the old SDP numbers with a different
 * wire type, so a record written by 2.x is never touched. The one ambiguous case, KeyInfo field 20 (SDP
 * compressionType / upstream ownerName, both strings), is guarded by running the migration once per DB
 * (see {@link SdpProtoMigration}).
 */
final class SdpProtoRewriter {
  static final int VARINT = 0;
  static final int FIXED64 = 1;
  static final int LEN = 2;
  static final int FIXED32 = 5;

  /** KeyInfo: 20 compressionType → 1001, 21 originalDataSize → 1002, 22 ownerName → upstream ownerName 20. */
  static final SdpProtoRewriter KEY_INFO = new SdpProtoRewriter("KeyInfo")
      .renumber(20, LEN, 1001).renumber(21, VARINT, 1002).renumber(22, LEN, 20)
      .skipIfAnyField(1001, 1002).skipIfField(21, LEN).skipIfField(22, VARINT);
  /** BucketInfo: 21 compressionType → 1001, 22 raftGroup → 1002. */
  static final SdpProtoRewriter BUCKET_INFO = new SdpProtoRewriter("BucketInfo")
      .renumber(21, LEN, 1001).renumber(22, LEN, 1002)
      .skipIfAnyField(1001, 1002);
  /** RepeatedKeyInfo (deletedTable): repeated KeyInfo keyInfo = 1. */
  static final SdpProtoRewriter REPEATED_KEY_INFO = new SdpProtoRewriter("RepeatedKeyInfo").nested(1, KEY_INFO);
  /** PartKeyInfo: required KeyInfo partKeyInfo = 3. */
  static final SdpProtoRewriter PART_KEY_INFO = new SdpProtoRewriter("PartKeyInfo").nested(3, KEY_INFO);
  /** MultipartKeyInfo (multipartInfoTable): repeated PartKeyInfo partKeyInfoList = 5. */
  static final SdpProtoRewriter MULTIPART_KEY_INFO = new SdpProtoRewriter("MultipartKeyInfo")
      .nested(5, PART_KEY_INFO);

  private final String name;
  // key: field << 3 | wire type
  private final Map<Integer, Integer> renumber = new HashMap<>();
  private final Map<Integer, SdpProtoRewriter> nestedTypes = new HashMap<>();
  private final Map<Integer, Integer> skipFieldsAnyType = new HashMap<>();
  private final Map<Integer, Integer> skipTags = new HashMap<>();

  private SdpProtoRewriter(String name) {
    this.name = name;
  }

  private SdpProtoRewriter renumber(int field, int wireType, int newField) {
    renumber.put(tag(field, wireType), newField);
    return this;
  }

  private SdpProtoRewriter nested(int field, SdpProtoRewriter type) {
    nestedTypes.put(field, type);
    return this;
  }

  /** A record containing any of these fields is already migrated (or written by 2.x). */
  private SdpProtoRewriter skipIfAnyField(int... fields) {
    for (int f : fields) {
      skipFieldsAnyType.put(f, f);
    }
    return this;
  }

  /** A record containing this field with this wire type was written by 2.x. */
  private SdpProtoRewriter skipIfField(int field, int wireType) {
    skipTags.put(tag(field, wireType), field);
    return this;
  }

  String getName() {
    return name;
  }

  static int tag(int field, int wireType) {
    return field << 3 | wireType;
  }

  /** Statistics of a migration run. */
  static final class Stats {
    private final Map<String, Long> counters = new TreeMap<>();
    private final Map<String, Long> movedCompressionValues = new TreeMap<>();

    void inc(String counter) {
      counters.merge(counter, 1L, Long::sum);
    }

    void compressionValue(String value) {
      movedCompressionValues.merge(value, 1L, Long::sum);
    }

    Map<String, Long> getCounters() {
      return Collections.unmodifiableMap(counters);
    }

    Map<String, Long> getMovedCompressionValues() {
      return Collections.unmodifiableMap(movedCompressionValues);
    }

    long get(String counter) {
      return counters.getOrDefault(counter, 0L);
    }
  }

  /**
   * @return the rewritten value, or {@code null} if the value needs no change
   */
  byte[] rewrite(byte[] value, Stats stats) {
    Map<Integer, Integer> present = scanTags(value);
    for (int t : present.keySet()) {
      if (skipFieldsAnyType.containsKey(t >>> 3) || skipTags.containsKey(t)) {
        stats.inc(name + ".skipped");
        return null;
      }
    }
    boolean changed = false;
    ByteArrayOutputStream out = new ByteArrayOutputStream(value.length + 16);
    Reader in = new Reader(value);
    while (in.hasMore()) {
      int t = in.readVarint32();
      int field = t >>> 3;
      int wireType = t & 7;
      Integer newField = renumber.get(t);
      SdpProtoRewriter nestedType = wireType == LEN ? nestedTypes.get(field) : null;
      if (nestedType != null) {
        byte[] payload = in.readLengthDelimited();
        byte[] rewritten = nestedType.rewrite(payload, stats);
        writeVarint(out, t);
        byte[] result = rewritten != null ? rewritten : payload;
        writeVarint(out, result.length);
        out.write(result, 0, result.length);
        changed |= rewritten != null;
      } else if (newField != null) {
        int start = in.pos;
        in.skipValue(wireType);
        if (name.equals("KeyInfo") && field == 20 || name.equals("BucketInfo") && field == 21) {
          stats.compressionValue(lengthDelimitedString(value, start));
        }
        writeVarint(out, tag(newField, wireType));
        out.write(value, start, in.pos - start);
        changed = true;
      } else {
        int start = in.pos;
        in.skipValue(wireType);
        writeVarint(out, t);
        out.write(value, start, in.pos - start);
      }
    }
    if (changed) {
      stats.inc(name + ".migrated");
      return out.toByteArray();
    }
    stats.inc(name + ".unchanged");
    return null;
  }

  private static Map<Integer, Integer> scanTags(byte[] value) {
    Map<Integer, Integer> tags = new HashMap<>();
    Reader in = new Reader(value);
    while (in.hasMore()) {
      int t = in.readVarint32();
      tags.put(t, t);
      in.skipValue(t & 7);
    }
    return tags;
  }

  private static String lengthDelimitedString(byte[] value, int start) {
    Reader r = new Reader(value);
    r.pos = start;
    int len = r.readVarint32();
    return new String(value, r.pos, len, StandardCharsets.UTF_8);
  }

  static void writeVarint(ByteArrayOutputStream out, long v) {
    while ((v & ~0x7FL) != 0) {
      out.write((int) ((v & 0x7F) | 0x80));
      v >>>= 7;
    }
    out.write((int) v);
  }

  /** Minimal protobuf wire reader. */
  static final class Reader {
    private final byte[] buf;
    private int pos;

    Reader(byte[] buf) {
      this.buf = buf;
    }

    boolean hasMore() {
      return pos < buf.length;
    }

    long readVarint() {
      long result = 0;
      for (int shift = 0; shift < 64; shift += 7) {
        if (pos >= buf.length) {
          throw new IllegalArgumentException("Truncated varint");
        }
        byte b = buf[pos++];
        result |= (long) (b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
          return result;
        }
      }
      throw new IllegalArgumentException("Malformed varint");
    }

    int readVarint32() {
      return (int) readVarint();
    }

    byte[] readLengthDelimited() {
      int len = readVarint32();
      if (len < 0 || pos + len > buf.length) {
        throw new IllegalArgumentException("Truncated length-delimited field");
      }
      byte[] payload = new byte[len];
      System.arraycopy(buf, pos, payload, 0, len);
      pos += len;
      return payload;
    }

    void skipValue(int wireType) {
      switch (wireType) {
      case VARINT:
        readVarint();
        break;
      case FIXED64:
        pos += 8;
        break;
      case LEN:
        int len = readVarint32();
        pos += len;
        break;
      case FIXED32:
        pos += 4;
        break;
      default:
        throw new IllegalArgumentException("Unsupported wire type " + wireType);
      }
      if (pos > buf.length) {
        throw new IllegalArgumentException("Truncated field");
      }
    }
  }
}
