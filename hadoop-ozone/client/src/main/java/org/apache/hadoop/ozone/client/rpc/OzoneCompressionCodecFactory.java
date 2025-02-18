/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone.client.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.utils.LegacyHadoopConfigurationSource;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.ozone.om.exceptions.OMException;

import java.io.IOException;

/**
 * Factory to create compression codecs.
 */
public final class OzoneCompressionCodecFactory {

  private OzoneCompressionCodecFactory() {
  }

  public static CompressionCodec getCompressionCodec(
      ConfigurationSource conf, String codecName)
      throws IOException {

    Configuration hadoopConfig =
        LegacyHadoopConfigurationSource.asHadoopConfiguration(conf);
    CompressionCodec codec =
        new CompressionCodecFactory(hadoopConfig)
            .getCodecByName(codecName);
    if (codec == null) {
      //compression type was set, but we failed to initialize the codec
      throw new OMException("Failed to initialize compression of type " +
          codecName,
          OMException.ResultCodes.COMPRESSION_NOT_SUPPORTED);
    }
    return codec;
  }

  public static String validateCompression(ConfigurationSource conf, String codecName) throws IOException {
    if (codecName == null) {
      return null;
    }
    // If codecName is not recognized, this method will throw an exception.
    getCompressionCodec(conf, codecName);

    return codecName;
  }
}
