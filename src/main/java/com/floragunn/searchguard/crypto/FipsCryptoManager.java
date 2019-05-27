package com.floragunn.searchguard.crypto;

import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivilegedAction;
import java.security.Provider;
import java.security.Provider.Service;
import java.security.PublicKey;
import java.security.Security;
import java.security.Signature;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.ssl.ExternalSearchGuardKeyStore;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.SgUtils;
import com.google.common.collect.ImmutableList;


final class FipsCryptoManager extends AbstractCryptoManager {

    private static final List<String> ALLOWED_FIPS_TLS_CHIPERS  = ImmutableList.of("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", 
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", 
            "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA", 
            "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA", 
            "TLS_RSA_WITH_AES_128_CBC_SHA256", 
            "TLS_RSA_WITH_AES_128_CBC_SHA",
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384", 
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384", 
            "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA", 
            "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA", 
            "TLS_RSA_WITH_AES_256_CBC_SHA256", 
            "TLS_RSA_WITH_AES_256_CBC_SHA");
    
    private static final List<String> ALLOWED_FIPS_TLS_PROTOCOLS  = ImmutableList.of("TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1");
    
    private Map<String, String> fipsKeystores;
    private Map<String, String> fipsMessageDigests;
    private Map<String, String> fipsSecretKeyFactories;
    private Map<String, String> fipsSignatures;
    private String fipsKeyStoreType;
    
    
    public FipsCryptoManager() {
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            @Override
            public Object run() {
                checkForNoBCProvider();
                evaluateProviders();
                checkSunJSSEProvider();
                return null;
            }
        });
    }

    @Override
    public KeyStore getKeystoreInstance(String keystoreType) throws KeyStoreException, NoSuchProviderException {
        return KeyStore.getInstance(fipsKeyStoreType);
    }

    @Override
    public String generatePasswordHash(char[] plainTextPassword) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        return super.generatePBKDF2PasswordHash(plainTextPassword, this.fipsSecretKeyFactories.get("PBKDF2WithHmacSHA512".toUpperCase()));
    }

    @Override
    public boolean checkPasswordHash(String hash, char[] plainTextPassword) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        return checkPBKDF2PasswordHash(hash, plainTextPassword, this.fipsSecretKeyFactories.get("PBKDF2WithHmacSHA512".toUpperCase()));
    }

    @Override
    public byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException, NoSuchProviderException {
        return MessageDigest.getInstance(algo, fipsMessageDigests.get(algo.toUpperCase())).digest(in);
    }

    @Override
    public byte[] fastHash(byte[] in, byte[] defaultSalt) throws NoSuchAlgorithmException, NoSuchProviderException {
        return hash(in, "SHA3-256");
    }
    
    private static final byte[] PUBLIC_KEY_BYTES = org.apache.commons.codec.binary.Base64.decodeBase64( 
            "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA5InMpuyrwqg/XJrj0PzR\n" + 
            "phU1/Mhp+CiqhSkks4grTes0zQfPiT2Ma9pSzhtj+5PpiB4Sc4U8m3s8V+yHYWuQ\n" + 
            "o2FZYfUnroxkcUh2Zmz7vYN/F680n9M1reqHdHc08buDXIJdBGpSm6czn4YZeFCQ\n" + 
            "fwokkkC9BeR+zIFDXlH73S/rOBlTqawttvolYg1iUeNkWQYPz6cIHrJ5yFW8n0s3\n" + 
            "HpenF4Og3M402hMBoFu+KDZt0bQUKNUh/vNwfqV+PyuJNEdYv5jKNZGw4b9oQCtX\n" + 
            "Nl4o7sAcINIy0XaQ5OM2JAsnC/EHHYU8odvUooCWt9JqbwJhCg1R5QqCS37Itt7+\n" + 
            "2wIDAQAB");
    
    private static final PublicKey PUBLIC_KEY;
    
    static {
        
        try {
            X509EncodedKeySpec ks = new X509EncodedKeySpec(PUBLIC_KEY_BYTES);
            KeyFactory kf = KeyFactory.getInstance("RSA");
            PUBLIC_KEY = kf.generatePublic(ks);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String validateLicense(final String base64EncodedLicense) throws Exception {
        final String base64DecodedLicense = new String(Base64.decodeBase64(base64EncodedLicense), StandardCharsets.UTF_8);
        final String jsonString = SgUtils.substringBetween(base64DecodedLicense, "{","}\n", true);
        final String signatureString = SgUtils.substringBetween(base64DecodedLicense, "<RSA>","\n</RSA>", false);
        
        final Signature signature = Signature.getInstance("SHA512withRSA", this.fipsSignatures.get("SHA512withRSA".toUpperCase())); 
        signature.initVerify(PUBLIC_KEY);
        signature.update(jsonString.getBytes(StandardCharsets.UTF_8));

        if(signature.verify(Base64.decodeBase64(signatureString))) {
            return jsonString;
        } else {
            throw new Exception("Invalid license signature");
        }
    }

    @Override
    public void checkTlsProtocols(List<String> tlsProtocols) {
        List<String> tmp = new ArrayList<String>(tlsProtocols);
        tmp.removeAll(FipsCryptoManager.ALLOWED_FIPS_TLS_PROTOCOLS);
        if(!tmp.isEmpty()) {
            throw new RuntimeException("Non fips compliant SSL/TLS protocols configured: "+tmp);
        }
    }

    @Override
    public void checkTlsChipers(List<String> tlsChipers) {
        List<String> tmp = new ArrayList<String>(tlsChipers);
        tmp.removeAll(FipsCryptoManager.ALLOWED_FIPS_TLS_CHIPERS);
        
        if(!tmp.isEmpty()) {
            throw new RuntimeException("Non fips compliant SSL/TLS chipers configured: "+tmp);
        }
    }

    @Override
    public void validateSettings(Settings settings) {
        throwIfTrue(settings, true, SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE);
        throwIfTrue(settings, true, SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE);
        
        if(ExternalSearchGuardKeyStore.hasExternalSslContext(settings)) {
            throw new RuntimeException("In FIPS mode '"+SSLConfigConstants.SEARCHGUARD_SSL_CLIENT_EXTERNAL_CONTEXT_ID+"' can not be set");
        }
    }
    
    @Override
    public List<String> getDefaultTlsCiphers() {
        return FipsCryptoManager.ALLOWED_FIPS_TLS_CHIPERS;
    }
    
    @Override
    public List<String> getDefaultTlsProtocols() {
        return AbstractCryptoManager.SECURE_TLS_PROTOCOLS;
    }

    @Override
    public boolean isHttpsClientCertRevocationSuported() {
        return false;
    }

    @Override
    public boolean isOpenSslAvailable() {
        return false;
    }

    private void throwIfTrue(Settings settings, boolean defaultValue, String boolSetting) {
        if(settings.getAsBoolean(boolSetting, defaultValue)) {
            throw new RuntimeException("In FIPS mode '"+boolSetting+"' can not be 'true'");
        }
    }

    //### helpers

    private void evaluateProviders() {

        final Map<String, String> _fipsKeystores = new HashMap<>();
        final Map<String, String> _fipsMessageDigests = new HashMap<>();
        final Map<String, String> _fipsSecretKeyFactories = new HashMap<>();
        final Map<String, String> _fipsSignatures = new HashMap<>();
        final List<String> _providerNames = new ArrayList<>();

        final Provider[] providerList = Security.getProviders();
        
        for (Provider provider : providerList) {
            
            _providerNames.add(provider.getName());
            LOGGER.info("Security provider: {} ({}) with {}", provider.getName(), provider.getVersion(), provider.getInfo());

            Set<Service> serviceList = provider.getServices();
            for (Service service : serviceList) {
                if (service.getType().equalsIgnoreCase("KeyStore")) {
                    if (fipsProvider(provider) /*&& !service.getAlgorithm().equalsIgnoreCase("PKCS12")*/) {
                        _fipsKeystores.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }

                if (service.getType().equalsIgnoreCase("MessageDigest")) {
                    if (fipsProvider(provider) && !service.getAlgorithm().equalsIgnoreCase("MD5")) {
                        _fipsMessageDigests.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }

                if (service.getType().equalsIgnoreCase("SecretKeyFactory")) {
                    if (fipsProvider(provider)) {
                        _fipsSecretKeyFactories.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }
                
                if (service.getType().equalsIgnoreCase("Signature")) {
                    if (fipsProvider(provider)) {
                        _fipsSignatures.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }
            }

        }
        
        LOGGER.info("Found {} installed security providers: {}", _providerNames.size(), _providerNames);
        this.fipsKeystores = Collections.unmodifiableMap(_fipsKeystores);
        this.fipsMessageDigests = Collections.unmodifiableMap(_fipsMessageDigests);
        this.fipsSecretKeyFactories = Collections.unmodifiableMap(_fipsSecretKeyFactories);
        this.fipsSignatures = Collections.unmodifiableMap(_fipsSignatures);
        
        LOGGER.info("FIPS keystores: {}", this.fipsKeystores);
        LOGGER.info("FIPS message disgests: {}", this.fipsMessageDigests);
        LOGGER.info("FIPS secret key factories: {}", this.fipsSecretKeyFactories);
        LOGGER.info("FIPS signatures: {}", this.fipsSignatures);
        
        fipsKeyStoreType = evaluateDefaultKeyStoreType();
        Security.setProperty("keystore.type", fipsKeyStoreType);
    }
    
    private boolean fipsProvider(Provider provider) {
        if(provider.getName().toUpperCase().contains("SUN") && !provider.getName().equals("SunJSSE")) {
            return false;
        }
        
        if(provider.getName().toUpperCase().contains("APPLE")) {
            return false;
        }
        
        return true;
    }
    
    private void checkSunJSSEProvider() {
        if (Security.getProvider("SunJSSE") == null) {
            LOGGER.info("No SunJSSE provider found");
        } else {
            
            LOGGER.info(Security.getProvider("SunJSSE").getInfo());
            
            if(!Security.getProvider("SunJSSE").getInfo().contains("Sun JSSE provider (FIPS mode, crypto provider")) {
                throw new RuntimeException("SunJSSE not in FIPS mode -> "+Security.getProvider("SunJSSE").getInfo());
            }
        }
    }
    
    private String evaluateDefaultKeyStoreType() {
        final String defaultKeyStoreType = Security.getProperty("keystore.type");
        
        if(defaultKeyStoreType == null) {
            LOGGER.warn("No default keystore.type set");
            return evaluateKeyStoreTypeFromDetectedTypes();
        } else if (defaultKeyStoreType.toLowerCase().contains("PKCS12") || defaultKeyStoreType.toLowerCase().contains("jks")) {
            LOGGER.warn("FIPS incompatible default keystore.type set ({})", defaultKeyStoreType);
            return evaluateKeyStoreTypeFromDetectedTypes();
        } else {
            LOGGER.info("Use keystore.type '{}' as default keystore type", defaultKeyStoreType);
            return defaultKeyStoreType;
        }
    }
    
    private String evaluateKeyStoreTypeFromDetectedTypes() {
        
        if(this.fipsKeystores.isEmpty()) {
            throw new RuntimeException("No FIPS compatible keystore type found");
        } else if(fipsKeystores.size() > 1) {
            final String kstype = this.fipsKeystores.keySet().iterator().next();
            LOGGER.warn("Multiple FIPS compatible keystore types found ({}). It's recommended to set keystore.type in your java security file. "
                    + "Will use: {}", this.fipsKeystores, kstype);
            return kstype;
        } else {
            final String kstype = this.fipsKeystores.keySet().iterator().next();
            LOGGER.info("Use FIPS compatible keystore type '{}' as default", kstype);
            return kstype;
        }
    }
    
    private void checkForNoBCProvider() {
        try {
            Class.forName("org.bouncycastle.crypto.digests.Blake2bDigest");
            throw new RuntimeException("Found non-FIPS compliant BouncyCastle class 'org.bouncycastle.crypto.digests.Blake2bDigest' on classpath. That is not permitted in FIPS mode. You need to remove them.");
        } catch (ClassNotFoundException e) {
          //ok
        }
        
        try {
            Class.forName("org.bouncycastle.jce.provider.BouncyCastleProvider");
            throw new RuntimeException("Found non-FIPS compliant BouncyCastle class 'org.bouncycastle.jce.provider.BouncyCastleProvider' on classpath. That is not permitted in FIPS mode. You need to remove them.");
        } catch (ClassNotFoundException e) {
          //ok
        }
        
        try {
            Class.forName("org.bouncycastle.openpgp.PGPException");
            throw new RuntimeException("Found non-FIPS compliant BouncyCastle class 'org.bouncycastle.openpgp.PGPException' on classpath. That is not permitted in FIPS mode. You need to remove them.");
        } catch (ClassNotFoundException e) {
            //ok
        }
    }

}
