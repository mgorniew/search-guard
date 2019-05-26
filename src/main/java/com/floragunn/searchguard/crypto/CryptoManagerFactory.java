package com.floragunn.searchguard.crypto;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.SpecialPermission;

public final class CryptoManagerFactory {
    
    private static final Logger LOGGER = LogManager.getLogger(CryptoManagerFactory.class);
    public static final List<String> JDK_SUPPORTED_SSL_CIPHERS;
    public static final List<String> JDK_SUPPORTED_SSL_PROTOCOLS;
    public static final boolean JDK_SUPPORTS_TLSV13;
    
    static {
        SSLEngine engine = null;
        List<String> jdkSupportedCiphers = Collections.emptyList();
        List<String> jdkSupportedProtocols = Collections.emptyList();
        boolean tls13Supported = false;
        try {
            final SSLContext serverContext = SSLContext.getInstance("TLS");
            serverContext.init(null, null, null);
            engine = serverContext.createSSLEngine();
            jdkSupportedCiphers = Arrays.asList(engine.getEnabledCipherSuites());
            jdkSupportedProtocols = Arrays.asList(engine.getEnabledProtocols());
            LOGGER.debug("JVM supports the following {} protocols {}", jdkSupportedProtocols.size(),
                    jdkSupportedProtocols);
            LOGGER.debug("JVM supports the following {} ciphers {}", jdkSupportedCiphers.size(),
                    jdkSupportedCiphers);
            
            if(jdkSupportedProtocols.contains("TLSv1.3")) {
                tls13Supported = true;
                LOGGER.info("JVM supports TLSv1.3");
            }
            
        } catch (final Throwable e) {
            LOGGER.error("Unable to determine supported ciphers due to " + e, e);
        } finally {
            if (engine != null) {
                try {
                    engine.closeInbound();
                } catch (Throwable e) {
                    LOGGER.debug("Unable to close inbound ssl engine", e);
                }
                
                try {
                    engine.closeOutbound();
                } catch (Throwable e) {
                    LOGGER.debug("Unable to close outbound ssl engine", e);
                }
                
            }
        }
        
        
        JDK_SUPPORTED_SSL_CIPHERS = Collections.unmodifiableList(jdkSupportedCiphers);
        JDK_SUPPORTED_SSL_PROTOCOLS = Collections.unmodifiableList(jdkSupportedProtocols);
        JDK_SUPPORTS_TLSV13 = tls13Supported;
    }
    
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
    
    public synchronized static Boolean ensureInitialized(final boolean fipsEnabled) {
        if(isInitialized()) {
            return isFipsEnabled()?Boolean.TRUE:Boolean.FALSE;
        } else {
            initialize(fipsEnabled);
            return null;
        }
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
        
        try {
            SSLContext.getDefault();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create a default context");
        }
        
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
