package com.floragunn.searchguard.auth.limiting;

import java.net.InetAddress;
import java.nio.file.Path;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.auth.blocking.HeapBasedClientBlockRegistry;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.util.ratetracking.RateTracker;

public abstract class AbstractRateLimiter<ClientIdType> implements AuthFailureListener, ClientBlockRegistry<ClientIdType> {
    protected final ClientBlockRegistry<ClientIdType> clientBlockRegistry;
    protected final RateTracker<ClientIdType> rateTracker;

    public AbstractRateLimiter(Settings settings, Path configPath, Class<ClientIdType> clientIdType) {
        this.clientBlockRegistry = new HeapBasedClientBlockRegistry<>(settings.getAsInt("block_expiry_seconds", 60 * 10) * 1000,
                settings.getAsInt("max_blocked_clients", 100_000), clientIdType);
        this.rateTracker = RateTracker.create(settings.getAsInt("time_window_seconds", 60 * 60) * 1000, settings.getAsInt("allowed_tries", 10),
                settings.getAsInt("max_tracked_clients", 100_000));
    }

    @Override
    public abstract void onAuthFailure(InetAddress remoteAddress, AuthCredentials authCredentials, Object request);

    @Override
    public boolean isBlocked(ClientIdType clientId) {
        return clientBlockRegistry.isBlocked(clientId);
    }

    @Override
    public void block(ClientIdType clientId) {
        clientBlockRegistry.block(clientId);
        rateTracker.reset(clientId);
    }

    @Override
    public Class<ClientIdType> getClientIdType() {
        return clientBlockRegistry.getClientIdType();
    }
}
