package com.github.pwrlabs.dbm;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.concurrent.TimeUnit;

public class TimedCache {
    private static Cache<String, Object> cache = CacheBuilder.newBuilder()
            .expireAfterAccess(2, TimeUnit.SECONDS) // Evict 2 seconds after last access
            .build();

    public static void put(String key, Object value) {
        cache.put(key, value);
    }

    public static Object get(String key) {
        return cache.getIfPresent(key);
    }
}

