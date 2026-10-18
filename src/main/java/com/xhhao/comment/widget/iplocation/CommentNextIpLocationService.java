package com.xhhao.comment.widget.iplocation;

import com.google.common.net.InetAddresses;
import com.xhhao.comment.widget.SettingConfigGetter;
import com.xhhao.comment.widget.network.CommentNextOutboundUriPolicy;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

/**
 * 把评论 IP 解析为属地文本，例如「广东」「日本」。
 *
 * <p>启用离线解析时优先使用内置的 ip2region 数据，未命中时再查询配置的在线接口；
 * 两边都拿不到结果、或 IP 不是公网地址时返回空字符串，调用方据此决定不展示属地。
 * 同一个 IP 的并发请求会合并，在线请求数量受限；离线查询不占用在线请求名额。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CommentNextIpLocationService {

    private static final int NEGATIVE_CACHE_MINUTES = 15;

    private static final int MIN_CACHE_MINUTES = 1;

    private static final int MAX_CACHE_MINUTES = 43_200;

    private static final int MIN_TIMEOUT_MILLIS = 500;

    private static final int MAX_TIMEOUT_MILLIS = 10_000;

    private static final int MAX_IN_FLIGHT_REQUESTS = 8;

    private final CommentNextIpLocationClient client;

    private final CommentNextIpLocationCache cache;

    private final CommentNextIpRegionDatabase ipRegionDatabase;

    private final SettingConfigGetter settingConfigGetter;

    private final Map<String, Mono<String>> inFlightRequests = new ConcurrentHashMap<>();

    private final Semaphore onlineRequests = new Semaphore(MAX_IN_FLIGHT_REQUESTS);

    /**
     * 解析属地，永不返回错误。
     */
    public Mono<String> resolve(String ipAddress) {
        if (!isPublicIp(ipAddress)) {
            return Mono.just("");
        }

        var normalizedIp = ipAddress.strip();
        return settingConfigGetter.getIpLocationConfig()
            .flatMap(config -> resolve(normalizedIp, config))
            .onErrorResume(error -> {
                log.debug("Failed to load IP location config.", error);
                return Mono.just("");
            });
    }

    private Mono<String> resolve(String ipAddress, SettingConfigGetter.IpLocationConfig config) {
        if (config == null || !config.isEnabled()) {
            return Mono.just("");
        }

        var apiUrlTemplate = resolveApiUrlTemplate(config);
        var cached = cache.get(apiUrlTemplate, ipAddress, config.isOfflineFallback());
        if (cached.isPresent()) {
            return Mono.just(cached.get());
        }

        return offlineLocation(ipAddress, config)
            .map(CommentNextIpLocationText::normalize)
            .filter(StringUtils::hasText)
            .doOnNext(location -> cache.put(apiUrlTemplate, ipAddress, config.isOfflineFallback(),
                location, cacheMinutes(config)))
            .switchIfEmpty(Mono.defer(() -> resolveOnline(ipAddress, apiUrlTemplate, config)));
    }

    private Mono<String> resolveOnline(String ipAddress, String apiUrlTemplate,
        SettingConfigGetter.IpLocationConfig config) {
        var key = apiUrlTemplate + '\0' + config.isOfflineFallback() + '\0' + ipAddress;
        var pending = inFlightRequests.get(key);
        if (pending != null) {
            return pending;
        }
        var request = inFlightRequests.computeIfAbsent(key, ignored -> {
            if (!onlineRequests.tryAcquire()) {
                return null;
            }
            return fetchLocation(key, ipAddress, apiUrlTemplate, config);
        });
        return request == null ? Mono.just("") : request;
    }

    private Mono<String> fetchLocation(String key, String ipAddress, String apiUrlTemplate,
        SettingConfigGetter.IpLocationConfig config) {
        return onlineLocation(ipAddress, apiUrlTemplate, config)
            .map(CommentNextIpLocationText::normalize)
            .switchIfEmpty(Mono.just(""))
            .doOnNext(location -> cache.put(
                apiUrlTemplate,
                ipAddress,
                config.isOfflineFallback(),
                location,
                location.isBlank() ? NEGATIVE_CACHE_MINUTES : cacheMinutes(config)
            ))
            .doFinally(signal -> {
                inFlightRequests.remove(key);
                onlineRequests.release();
            })
            .cache();
    }

    private Mono<String> onlineLocation(String ipAddress, String apiUrlTemplate,
        SettingConfigGetter.IpLocationConfig config) {
        return Mono.defer(() -> client.fetchLocation(ipAddress, apiUrlTemplate, timeout(config)))
            .filter(StringUtils::hasText)
            .onErrorResume(error -> {
                log.debug("Failed to resolve IP location from the configured API. ip={}",
                    ipAddress, error);
                return Mono.empty();
            });
    }

    private Mono<String> offlineLocation(String ipAddress,
        SettingConfigGetter.IpLocationConfig config) {
        if (!config.isOfflineFallback()) {
            return Mono.empty();
        }
        return ipRegionDatabase.lookup(ipAddress).filter(StringUtils::hasText);
    }

    private String resolveApiUrlTemplate(SettingConfigGetter.IpLocationConfig config) {
        return StringUtils.hasText(config.getApiUrlTemplate())
            ? config.getApiUrlTemplate().strip()
            : SettingConfigGetter.IpLocationConfig.DEFAULT_API_URL_TEMPLATE;
    }

    private Duration timeout(SettingConfigGetter.IpLocationConfig config) {
        return Duration.ofMillis(
            clamp(config.getTimeoutMillis(), MIN_TIMEOUT_MILLIS, MAX_TIMEOUT_MILLIS)
        );
    }

    private int cacheMinutes(SettingConfigGetter.IpLocationConfig config) {
        return clamp(config.getCacheMinutes(), MIN_CACHE_MINUTES, MAX_CACHE_MINUTES);
    }

    private int clamp(int value, int min, int max) {
        return Math.min(Math.max(value, min), max);
    }

    private boolean isPublicIp(String ipAddress) {
        if (!StringUtils.hasText(ipAddress)) {
            return false;
        }

        var candidate = ipAddress.strip();
        if (!InetAddresses.isInetAddress(candidate)) {
            return false;
        }

        return CommentNextOutboundUriPolicy.isPublicAddress(InetAddresses.forString(candidate));
    }
}
