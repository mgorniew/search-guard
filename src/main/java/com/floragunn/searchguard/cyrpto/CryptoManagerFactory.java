package com.floragunn.searchguard.cyrpto;

import java.util.concurrent.atomic.AtomicBoolean;

public class CryptoManagerFactory {
    
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
    
    //synchronized, multithreading
    public static void initialize(boolean fipsEnabled) {
        if(isInitialized()) {
            throw new RuntimeException("CryptoManagerFactory already initialized");
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
