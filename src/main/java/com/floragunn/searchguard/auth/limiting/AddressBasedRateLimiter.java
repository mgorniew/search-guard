package com.floragunn.searchguard.auth.limiting;

import java.net.InetAddress;
import java.nio.file.Path;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.user.AuthCredentials;

public class AddressBasedRateLimiter extends AbstractRateLimiter<InetAddress> implements AuthFailureListener, ClientBlockRegistry<InetAddress> {

    public AddressBasedRateLimiter(Settings settings, Path configPath) {
        super(settings, configPath, InetAddress.class);
    }

    @Override
    public void onAuthFailure(InetAddress remoteAddress, AuthCredentials authCredentials, Object request) {
        if (this.rateTracker.track(remoteAddress)) {
            block(remoteAddress);
        }
    }
}
