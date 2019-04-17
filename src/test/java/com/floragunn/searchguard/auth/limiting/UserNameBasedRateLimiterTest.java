package com.floragunn.searchguard.auth.limiting;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

public class UserNameBasedRateLimiterTest {

    @Test
    public void simpleTest() throws Exception {
        Settings settings = Settings.builder().put("allowed_tries", 3).build();

        AddressBasedRateLimiter rateLimiter = new AddressBasedRateLimiter(settings, null);

        assertFalse(rateLimiter.isBlocked(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 })));
        rateLimiter.onAuthFailure(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 }), null, null);
        assertFalse(rateLimiter.isBlocked(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 })));
        rateLimiter.onAuthFailure(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 }), null, null);
        assertFalse(rateLimiter.isBlocked(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 })));
        rateLimiter.onAuthFailure(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 }), null, null);
        assertTrue(rateLimiter.isBlocked(InetAddress.getByAddress(new byte[] { 1, 2, 3, 4 })));

    }
}
