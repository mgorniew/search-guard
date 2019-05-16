package com.floragunn.searchguard.crypto;

import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.SpecialPermission;

public final class CryptoManagerFactory {
    
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean();
    private static final AtomicBoolean FIPS_ENABLED = new AtomicBoolean();
    private static CryptoManager manager;
    
    public static boolean isInitialized() {
        return manager != null && INITIALIZED.get();
    }
    
    public static boolean isFipsEnabled() {
        checkInitialized();
        return FIPS_ENABLED.get();
    }
 
    public synchronized static void initialize(final boolean fipsEnabled) {
        if(isInitialized()) {
            throw new RuntimeException("CryptoManagerFactory already initialized");
        }
        
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        } else if (fipsEnabled) {
            throw new RuntimeException("No security manager installed");
        }
        
        if(fipsEnabled) {
            manager = new FipsCryptoManager();
        } else {
            manager = new DefaultCryptoManager();
        }

        FIPS_ENABLED.set(fipsEnabled);
        INITIALIZED.set(true);
    }
    
    private static void checkInitialized() {
        if(!isInitialized()) {
            throw new RuntimeException("CryptoManagerFactory not yet initialized");
        }
    }
    
    public static CryptoManager getInstance() {
        checkInitialized();
        return manager;
    }

}
