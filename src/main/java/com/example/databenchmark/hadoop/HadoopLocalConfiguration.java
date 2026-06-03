package com.example.databenchmark.hadoop;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

public final class HadoopLocalConfiguration {
    private HadoopLocalConfiguration() {}

    public static Configuration create() throws IOException {
        ensureHadoopHomeForWindows();
        Configuration configuration = new Configuration();
        configuration.setClass("fs.file.impl", NoPermissionRawLocalFileSystem.class, FileSystem.class);
        configuration.setBoolean("fs.file.impl.disable.cache", true);
        return configuration;
    }

    private static void ensureHadoopHomeForWindows() throws IOException {
        if (!System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win")
            || System.getProperty("hadoop.home.dir") != null) {
            return;
        }

        Path hadoopHome = Path.of(System.getProperty("java.io.tmpdir"), "data-benchmark-hadoop-home");
        Path bin = hadoopHome.resolve("bin");
        Path winutils = bin.resolve("winutils.exe");
        Files.createDirectories(bin);
        if (Files.notExists(winutils)) {
            Files.createFile(winutils);
        }
        System.setProperty("hadoop.home.dir", hadoopHome.toAbsolutePath().toString());
    }

    public static class NoPermissionRawLocalFileSystem extends RawLocalFileSystem {
        @Override
        public void setPermission(org.apache.hadoop.fs.Path path, FsPermission permission) {
            // Hadoop's Windows local FS shells out to winutils for chmod; benchmark output does not need it.
        }
    }
}
