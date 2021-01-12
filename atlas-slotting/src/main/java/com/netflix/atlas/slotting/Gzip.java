/*
 * Copyright 2014-2021 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.atlas.slotting;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

final class Gzip {
  private Gzip() {
  }

  static byte[] compressString(String str) throws IOException {
    return compress(str.getBytes(StandardCharsets.UTF_8));
  }

  static String decompressString(byte[] bytes) throws IOException {
    return new String(decompress(bytes), StandardCharsets.UTF_8);
  }

  private static byte[] compress(byte[] bytes) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (GZIPOutputStream out = new GZIPOutputStream(baos)) {
      out.write(bytes);
    }

    return baos.toByteArray();
  }

  private static byte[] decompress(byte[] bytes) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try (GZIPInputStream in = new GZIPInputStream(new ByteArrayInputStream(bytes))) {
      byte[] buffer = new byte[4096];
      int length;
      while ((length = in.read(buffer)) > 0) {
        baos.write(buffer, 0, length);
      }
    }

    return baos.toByteArray();
  }
}
