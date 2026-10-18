package com.xhhao.comment.widget.iplocation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import org.springframework.core.io.Resource;

/** 提取内置数据文件，以内容摘要隔离版本并校验临时目录中的副本。 */
final class CommentNextIpRegionDatabaseFiles {

    private CommentNextIpRegionDatabaseFiles() {
    }

    static File extract(Resource resource, Path directory) throws IOException {
        Files.createDirectories(directory);
        var checksum = checksum(resource.getInputStream());
        var target = directory.resolve(checksum + "-" + resource.getFilename());
        if (Files.isRegularFile(target)
            && checksum.equals(checksum(Files.newInputStream(target)))) {
            return target.toFile();
        }

        var temporary = Files.createTempFile(directory, "ip2region-", ".part");
        try {
            try (var input = resource.getInputStream()) {
                Files.copy(input, temporary, StandardCopyOption.REPLACE_EXISTING);
            }
            try {
                Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(temporary, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            Files.deleteIfExists(temporary);
        }
        return target.toFile();
    }

    private static String checksum(InputStream input) throws IOException {
        final MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            input.close();
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
        try (var stream = new DigestInputStream(input, digest)) {
            stream.transferTo(OutputStream.nullOutputStream());
        }
        return HexFormat.of().formatHex(digest.digest());
    }
}
