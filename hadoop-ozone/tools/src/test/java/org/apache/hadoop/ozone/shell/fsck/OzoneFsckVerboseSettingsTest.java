/*
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

package org.apache.hadoop.ozone.shell.fsck;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class OzoneFsckVerboseSettingsTest {
  @ParameterizedTest
  @MethodSource("verboseSettingsVariants")
  void testVerboseSettingsParsing(String level, boolean printHealthyKeys, boolean expectedPrintContainers,
      boolean expectedPrintBlocks, boolean expectedPrintChunks) {
    OzoneFsckVerboseSettings settings = OzoneFsckVerboseSettings.builder()
        .printHealthyKeys(false)
        .level(OzoneFsckVerbosityLevel.valueOf(level))
        .build();

    assertThat(settings.printHealthyKeys())
        .withFailMessage("Unexpected configuration for print healthy keys")
        .isEqualTo(printHealthyKeys);

    assertThat(settings.printContainers())
        .withFailMessage("Unexpected configuration for print container information")
        .isEqualTo(expectedPrintContainers);

    assertThat(settings.printBlocks())
        .withFailMessage("Unexpected configuration for print blocks information")
        .isEqualTo(expectedPrintBlocks);

    assertThat(settings.printChunks())
        .withFailMessage("Unexpected configuration for print chunks information")
        .isEqualTo(expectedPrintChunks);
  }

  private static Stream<Arguments> verboseSettingsVariants() {
    return Stream.of(
        arguments("KEY", false, false, false, false),
        arguments("KEY", false, false, false, false),
        arguments("CONTAINER", false, true, false, false),
        arguments("BLOCK", false, true, true, false),
        arguments("CHUNK", false, true, true, true)
    );
  }
}
