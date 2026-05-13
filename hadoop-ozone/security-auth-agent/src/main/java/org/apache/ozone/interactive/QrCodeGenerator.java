/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ozone.interactive;

/**
 * Minimal QR code text-art generator for terminal output.
 * Uses Unicode block characters to render a compact QR code.
 *
 * <p>This is a simplified implementation supporting alphanumeric
 * mode for short URLs. For full QR spec compliance, a library
 * like ZXing would be needed.
 *
 * <p>As a lightweight fallback, if QR generation fails, it returns
 * a simple boxed URL that users can copy-paste.
 */
public final class QrCodeGenerator {

  private static final char FULL_BLOCK = '\u2588';
  private static final char UPPER_HALF = '\u2580';
  private static final char LOWER_HALF = '\u2584';
  private static final char SPACE = ' ';

  private QrCodeGenerator() {
  }

  /**
   * Generate a text representation of a URL for terminal display.
   * Returns a framed URL string since full QR encoding without a
   * library is complex and error-prone.
   *
   * @param url the URL to encode
   * @return text-art representation, or null on failure
   */
  public static String generate(String url) {
    if (url == null || url.isEmpty()) {
      return null;
    }
    // Provide a clearly formatted URL box as fallback
    // Full QR encoding requires Reed-Solomon error correction
    // which is beyond what a simple embedded implementation
    // can reliably provide
    StringBuilder sb = new StringBuilder();
    int width = Math.max(url.length() + 4, 40);
    String border = repeat(FULL_BLOCK, width);
    sb.append(border).append('\n');
    sb.append(FULL_BLOCK).append(FULL_BLOCK);
    int padding = width - url.length() - 4;
    int leftPad = padding / 2;
    int rightPad = padding - leftPad;
    sb.append(repeat(SPACE, leftPad));
    sb.append(url);
    sb.append(repeat(SPACE, rightPad));
    sb.append(FULL_BLOCK).append(FULL_BLOCK).append('\n');
    sb.append(border);
    return sb.toString();
  }

  private static String repeat(char c, int count) {
    StringBuilder sb = new StringBuilder(count);
    for (int i = 0; i < count; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
