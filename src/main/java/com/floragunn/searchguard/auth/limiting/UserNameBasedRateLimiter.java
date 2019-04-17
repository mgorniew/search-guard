package com.floragunn.searchguard.auth.limiting;

import java.net.InetAddress;
import java.nio.file.Path;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.auth.AuthFailureListener;
import com.floragunn.searchguard.auth.blocking.ClientBlockRegistry;
import com.floragunn.searchguard.user.AuthCredentials;

public class UserNameBasedRateLimiter extends AbstractRateLimiter<String> implements AuthFailureListener, ClientBlockRegistry<String> {

    public UserNameBasedRateLimiter(Settings settings, Path configPath) {
        super(settings, configPath, String.class);
    }

    @Override
    public void onAuthFailure(InetAddress remoteAddress, AuthCredentials authCredentials, Object request) {
        if (authCredentials != null && authCredentials.getUsername() != null && this.rateTracker.track(authCredentials.getUsername())) {
            block(authCredentials.getUsername());
        }
    }
}
