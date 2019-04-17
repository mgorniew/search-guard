package com.floragunn.searchguard.auth.limiting;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import com.floragunn.searchguard.user.AuthCredentials;

public class AddressBasedRateLimiterTest {

    private final static byte[] PASSWORD = new byte[] { '1', '2', '3' };

    @Test
    public void simpleTest() throws Exception {
        Settings settings = Settings.builder().put("allowed_tries", 3).build();

        UserNameBasedRateLimiter rateLimiter = new UserNameBasedRateLimiter(settings, null);

        assertFalse(rateLimiter.isBlocked("a"));
        rateLimiter.onAuthFailure(null, new AuthCredentials("a", PASSWORD), null);
        assertFalse(rateLimiter.isBlocked("a"));
        rateLimiter.onAuthFailure(null, new AuthCredentials("a", PASSWORD), null);
        assertFalse(rateLimiter.isBlocked("a"));
        rateLimiter.onAuthFailure(null, new AuthCredentials("a", PASSWORD), null);
        assertTrue(rateLimiter.isBlocked("a"));

    }
}
