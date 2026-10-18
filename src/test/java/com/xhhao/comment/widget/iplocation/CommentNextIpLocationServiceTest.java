package com.xhhao.comment.widget.iplocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.xhhao.comment.widget.SettingConfigGetter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

class CommentNextIpLocationServiceTest {

    private final CommentNextIpLocationClient client = mock(CommentNextIpLocationClient.class);

    private final CommentNextIpRegionDatabase database = mock(CommentNextIpRegionDatabase.class);

    private final SettingConfigGetter settings = mock(SettingConfigGetter.class);

    private final SettingConfigGetter.IpLocationConfig config = new SettingConfigGetter.IpLocationConfig();

    private final CommentNextIpLocationService service = new CommentNextIpLocationService(
        client, new CommentNextIpLocationCache(), database, settings);

    @BeforeEach
    void setUp() {
        when(settings.getIpLocationConfig()).thenReturn(Mono.just(config));
        when(database.lookup(anyString())).thenReturn(Mono.empty());
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(Mono.empty());
    }

    @Test
    void localCommentAndReplyLookupsDoNotSubscribeToStalledOnlineInterface() {
        when(database.lookup(anyString())).thenReturn(Mono.just("美国"));
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(Mono.never());

        assertEquals("美国", service.resolve("8.8.8.8")
            .flatMap(comment -> service.resolve("1.1.1.1"))
            .block(Duration.ofMillis(500)));
        verifyNoInteractions(client);
    }

    @Test
    void moreThanEightUniqueAddressesCanUseLocalDatabase() {
        when(database.lookup(anyString())).thenReturn(Mono.just("美国"));

        for (int i = 1; i <= 24; i++) {
            assertEquals("美国", service.resolve("8.8.8." + i).block(Duration.ofMillis(500)));
        }
        verifyNoInteractions(client);
    }

    @Test
    void localMissUsesOnlineInterfaceAndCachesResult() {
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(Mono.just("日本"));

        assertEquals("日本", service.resolve("8.8.8.8").block());
        assertEquals("日本", service.resolve("8.8.8.8").block());
        verify(client, times(1)).fetchLocation(eq("8.8.8.8"), anyString(), eq(Duration.ofMillis(3000)));
    }

    @Test
    void disablingLocalDatabaseUsesOnlineInterface() {
        config.setOfflineFallback(false);
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(Mono.just("日本"));

        assertEquals("日本", service.resolve("8.8.8.8").block());
        verifyNoInteractions(database);
    }

    @Test
    void changingLookupModeDoesNotReuseCachedResultFromPreviousMode() {
        when(database.lookup(anyString())).thenReturn(Mono.just("美国"));
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(Mono.just("日本"));

        assertEquals("美国", service.resolve("8.8.8.8").block());
        config.setOfflineFallback(false);
        assertEquals("日本", service.resolve("8.8.8.8").block());
        config.setOfflineFallback(true);
        assertEquals("美国", service.resolve("8.8.8.8").block());
        verify(client, times(1)).fetchLocation(anyString(), anyString(), any());
    }

    @Test
    void disabledFeatureAndPrivateAddressesDoNotPerformLookups() {
        config.setEnabled(false);
        assertEquals("", service.resolve("8.8.8.8").block());
        config.setEnabled(true);
        for (var ip : new String[] {"127.0.0.1", "192.168.1.1", "not-an-ip", ""}) {
            assertEquals("", service.resolve(ip).block());
        }
        verifyNoInteractions(client, database);
    }

    @Test
    void failedOnlineRequestReturnsEmptyAndIsNegativeCached() {
        when(client.fetchLocation(anyString(), anyString(), any()))
            .thenReturn(Mono.error(new IllegalStateException("unavailable")));

        assertEquals("", service.resolve("8.8.8.8").block());
        assertEquals("", service.resolve("8.8.8.8").block());
        verify(client, times(1)).fetchLocation(anyString(), anyString(), any());
    }

    @Test
    void concurrentSubscribersShareOneOnlineRequest() {
        var response = Sinks.<String>one();
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(response.asMono());

        var first = service.resolve("8.8.8.8").toFuture();
        var second = service.resolve("8.8.8.8").toFuture();
        response.tryEmitValue("美国");

        assertEquals("美国", first.join());
        assertEquals("美国", second.join());
        verify(client, times(1)).fetchLocation(anyString(), anyString(), any());
    }

    @Test
    void saturatedOnlineRequestsDoNotBlockLocalLookupsOrCacheSkippedAddresses() {
        var response = Sinks.<String>one();
        when(client.fetchLocation(anyString(), anyString(), any())).thenReturn(response.asMono());
        var pending = new ArrayList<CompletableFuture<String>>();
        for (int i = 1; i <= 8; i++) {
            pending.add(service.resolve("8.8.8." + i).toFuture());
        }
        assertEquals("", service.resolve("8.8.8.9").block(Duration.ofMillis(500)));
        verify(client, never()).fetchLocation(eq("8.8.8.9"), anyString(), any());

        when(database.lookup("1.1.1.1")).thenReturn(Mono.just("澳大利亚"));
        assertEquals("澳大利亚", service.resolve("1.1.1.1").block(Duration.ofMillis(500)));

        response.tryEmitValue("美国");
        pending.forEach(future -> assertEquals("美国", future.join()));
        assertEquals("美国", service.resolve("8.8.8.9").block(Duration.ofMillis(500)));
        verify(client, times(9)).fetchLocation(anyString(), anyString(), any());
    }
}
