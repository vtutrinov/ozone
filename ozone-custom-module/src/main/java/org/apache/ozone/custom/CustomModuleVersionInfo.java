package org.apache.ozone.custom;

import org.apache.hadoop.hdds.utils.VersionInfo;

public class CustomModuleVersionInfo {
    public static final VersionInfo CUSTOM_MODULE_VERSION_INFO =
            new VersionInfo("custom-module");

    private CustomModuleVersionInfo() {
    }

    public static void main(String[] args) {
        System.out.println("Using Custom Module Lib " + CUSTOM_MODULE_VERSION_INFO.getVersion());
        System.out.println(
                "Source code repository " + CUSTOM_MODULE_VERSION_INFO.getUrl() + " -r " +
                        CUSTOM_MODULE_VERSION_INFO.getRevision());
        System.out.println("Compiled by " + CUSTOM_MODULE_VERSION_INFO.getUser() + " on "
                + CUSTOM_MODULE_VERSION_INFO.getDate());
        System.out.println(
                "Compiled with protoc " + CUSTOM_MODULE_VERSION_INFO.getHadoopProtoc2Version() +
                        ", " + CUSTOM_MODULE_VERSION_INFO.getGrpcProtocVersion() +
                        " and " + CUSTOM_MODULE_VERSION_INFO.getHadoopProtoc3Version());
        System.out.println(
                "From source with checksum " + CUSTOM_MODULE_VERSION_INFO.getSrcChecksum());
        System.out.println(
                "Compiled on platform " + CUSTOM_MODULE_VERSION_INFO.getCompilePlatform());
    }
}
