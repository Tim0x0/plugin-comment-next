package com.xhhao.comment.widget.iplocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.core.io.FileSystemResource;

class CommentNextIpRegionDatabaseFilesTest {

    @TempDir
    Path directory;

    @Test
    void newBundledContentsUseNewPathAndLeaveExistingDatabaseUntouched() throws Exception {
        var source = directory.resolve("ip2region_v4.xdb");
        Files.writeString(source, "first release");
        var resource = new FileSystemResource(source);
        var targetDirectory = directory.resolve("extracted");
        Files.createDirectories(targetDirectory);
        var legacy = targetDirectory.resolve("ip2region_v4.xdb");
        Files.writeString(legacy, "legacy fixed filename");

        var first = CommentNextIpRegionDatabaseFiles.extract(resource, targetDirectory);
        Files.writeString(source, "second release");
        var second = CommentNextIpRegionDatabaseFiles.extract(resource, targetDirectory);

        assertNotEquals(first, second);
        assertEquals("first release", Files.readString(first.toPath()));
        assertEquals("second release", Files.readString(second.toPath()));
        assertEquals("legacy fixed filename", Files.readString(legacy));
    }

    @Test
    void repairsNonemptyCorruptCopyAndReusesVerifiedCopy() throws Exception {
        var source = directory.resolve("ip2region_v4.xdb");
        Files.writeString(source, "bundled database contents");
        var resource = new FileSystemResource(source);
        var targetDirectory = directory.resolve("extracted");
        var extracted = CommentNextIpRegionDatabaseFiles.extract(resource, targetDirectory);
        Files.writeString(extracted.toPath(), "nonempty corrupt copy");

        var repaired = CommentNextIpRegionDatabaseFiles.extract(resource, targetDirectory);
        assertEquals(extracted, repaired);
        assertEquals("bundled database contents", Files.readString(repaired.toPath()));
        assertEquals(repaired, CommentNextIpRegionDatabaseFiles.extract(resource, targetDirectory));
        try (var files = Files.list(targetDirectory)) {
            assertEquals(1, files.count());
        }
    }
}
