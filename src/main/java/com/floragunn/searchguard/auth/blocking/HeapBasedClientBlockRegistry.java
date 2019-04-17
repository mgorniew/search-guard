package com.floragunn.searchguard.auth.blocking;

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class HeapBasedClientBlockRegistry<ClientIdType> implements ClientBlockRegistry<ClientIdType> {

    private final Logger log = LogManager.getLogger(this.getClass());

    private final Cache<ClientIdType, Long> cache;
    private final Class<ClientIdType> clientIdType;

    public HeapBasedClientBlockRegistry(long expiryMs, int maxEntries, Class<ClientIdType> clientIdType) {
        this.clientIdType = clientIdType;
        this.cache = CacheBuilder.newBuilder().expireAfterWrite(expiryMs, TimeUnit.MILLISECONDS).maximumSize(maxEntries).concurrencyLevel(4)
                .removalListener(new RemovalListener<ClientIdType, Long>() {
                    @Override
                    public void onRemoval(RemovalNotification<ClientIdType, Long> notification) {
                        if (log.isInfoEnabled()) {
                            log.info("Unblocking " + notification.getKey());
                        }
                    }
                }).build();
    }

    @Override
    public boolean isBlocked(ClientIdType clientId) {
        return cache.getIfPresent(clientId) != null;
    }

    @Override
    public void block(ClientIdType clientId) {
        if (log.isInfoEnabled()) {
            log.info("Blocking " + clientId);
        }

        this.cache.put(clientId, System.currentTimeMillis());
    }

    @Override
    public Class<ClientIdType> getClientIdType() {
        return clientIdType;
    }

}
