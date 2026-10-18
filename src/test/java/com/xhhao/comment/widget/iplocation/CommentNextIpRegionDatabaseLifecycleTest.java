package com.xhhao.comment.widget.iplocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lionsoul.ip2region.service.Ip2Region;
import org.lionsoul.ip2region.service.SearcherPool;
import org.lionsoul.ip2region.xdb.Searcher;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.test.util.ReflectionTestUtils;

class CommentNextIpRegionDatabaseLifecycleTest {

    @TempDir
    Path directory;

    @Test
    void contextShutdownClosesAllEightFilesAndPreventsReinitialization() throws Exception {
        var context = new AnnotationConfigApplicationContext();
        context.registerBean(CommentNextIpRegionDatabase.class,
            () -> new CommentNextIpRegionDatabase(directory));
        context.refresh();
        var database = context.getBean(CommentNextIpRegionDatabase.class);
        try {
            assertEquals("美国", database.lookup("8.8.8.8").block(Duration.ofSeconds(10)));
            var engine = (Ip2Region) ReflectionTestUtils.getField(database, "ip2Region");
            var handles = handles(engine);
            assertEquals(8, handles.size());
            for (var handle : handles) {
                assertTrue(handle.getFD().valid());
            }
            var deferredLookup = database.lookup("1.1.1.1");

            context.close();

            for (var handle : handles) {
                assertFalse(handle.getFD().valid(), "plugin shutdown must close the XDB handles");
            }
            assertEquals("", deferredLookup.block(Duration.ofSeconds(1)));
            assertEquals("", database.lookup("8.8.8.8").block(Duration.ofSeconds(1)));
            assertNull(ReflectionTestUtils.getField(database, "ip2Region"));
            database.destroy();
        } finally {
            context.close();
        }
    }

    @SuppressWarnings("unchecked")
    private static List<RandomAccessFile> handles(Ip2Region engine) {
        var handles = new ArrayList<RandomAccessFile>();
        for (var poolName : List.of("v4Pool", "v6Pool")) {
            var pool = (SearcherPool) ReflectionTestUtils.getField(engine, poolName);
            var searchers = (Queue<Searcher>) ReflectionTestUtils.getField(pool, "pool");
            for (var searcher : searchers) {
                handles.add((RandomAccessFile) ReflectionTestUtils.getField(searcher, "handle"));
            }
        }
        return handles;
    }
}
