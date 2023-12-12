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

package org.apache.ozone.custom;

import org.apache.hadoop.hdds.utils.VersionInfo;

/**
 * Version information of the SDP custom module.
 */
public final class CustomModuleVersionInfo {
  public static final VersionInfo CUSTOM_MODULE_VERSION_INFO = new VersionInfo("custom-module");

  private CustomModuleVersionInfo() {
  }

  public static void main(String[] args) {
    System.out.println("Using Custom Module Lib " + CUSTOM_MODULE_VERSION_INFO.getVersion());
    System.out.println("Source code repository " + CUSTOM_MODULE_VERSION_INFO.getUrl() + " -r "
        + CUSTOM_MODULE_VERSION_INFO.getRevision());
    System.out.println("Compiled with protoc " + CUSTOM_MODULE_VERSION_INFO.getProtoVersions());
    System.out.println("From source with checksum " + CUSTOM_MODULE_VERSION_INFO.getSrcChecksum());
    System.out.println("Compiled on platform " + CUSTOM_MODULE_VERSION_INFO.getCompilePlatform());
  }
}
