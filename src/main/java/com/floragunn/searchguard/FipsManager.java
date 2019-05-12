package com.floragunn.searchguard;

import java.security.AccessController;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Permission;
import java.security.Policy;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.security.Provider;
import java.security.Provider.Service;
import java.security.Security;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Hex;
import org.elasticsearch.SpecialPermission;

import com.google.common.collect.Lists;

public final class FipsManager {
    
    private static final Logger LOGGER = LogManager.getLogger(FipsManager.class);
    private static AtomicBoolean INITIALIZED = new AtomicBoolean();
    private static AtomicBoolean FIPS_ENABLED = new AtomicBoolean();
    
    private static Map<String, String> fipsKeystores;
    private static Map<String, String> fipsMessageDigests;
    private static Map<String, String> fipsSecretKeyFactories;
    private static String fipsKeyStoreType;
    
    //enterprise needed?
    //do not allow sslv3,sslv2 even when configured so
    
    public static KeyStore getKeystoreInstance(String keystoreType) throws KeyStoreException, NoSuchProviderException {
        checkInitialized();
        if(isFipsEnabled()) {
            return KeyStore.getInstance(fipsKeyStoreType);
        }
        return KeyStore.getInstance(keystoreType);
    }
    
    public static void checkTlsProtocols(List<String> tlsProtocols) {
        checkInitialized();
        if(isFipsEnabled()) {
            List<String> tmp = new ArrayList<String>(tlsProtocols);
            tmp.removeAll(Lists.newArrayList("TLSv1.3", "TLSv1.2", "TLSv1.1", "TLSv1"));
            if(!tmp.isEmpty()) {
                throw new RuntimeException("Non fips compliant SSL/TLS protocols configured: "+tmp);
            }
        }
        
        //Collections.unmodifiableList(
    }
    
    public static List<String> filterFipsTlsChipers(List<String> tlsChipers) {
        //https://www2.cs.duke.edu/csed/java/jdk1.8/technotes/guides/security/jsse/FIPS.html
        checkInitialized();
        if(isFipsEnabled()) {
           
            List<String> tmp = new ArrayList<String>(tlsChipers);
            tmp.retainAll(Lists.newArrayList("TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", 
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
                    "TLS_RSA_WITH_AES_256_CBC_SHA"));
            
            
            if(tmp.isEmpty()) {
                throw new RuntimeException("Non fips compliant SSL/TLS chipers configured "+tlsChipers);
            } else {
                LOGGER.info("FIPS compatible enabled cipher suites: {}", tmp);
            }
            
            return Collections.unmodifiableList(tmp);
            
        } else {
            return Collections.unmodifiableList(tlsChipers);
        }
    }
    
    private static void evaluateProviders() {

        final Map<String, String> _fipsKeystores = new HashMap<>();
        final Map<String, String> _fipsMessageDigests = new HashMap<>();
        final Map<String, String> _fipsSecretKeyFactories = new HashMap<>();
        final List<String> _providerNames = new ArrayList<>();

        final Provider[] providerList = Security.getProviders();
        
        for (Provider provider : providerList) {
            
            _providerNames.add(provider.getName());

            Set<Service> serviceList = provider.getServices();
            for (Service service : serviceList) {

                if (service.getType().equalsIgnoreCase("KeyStore")) {
                    if (!provider.getName().equalsIgnoreCase("SUN") && !provider.getName().equalsIgnoreCase("SunJCE")
                            && !provider.getName().equalsIgnoreCase("Apple") && !service.getAlgorithm().equalsIgnoreCase("PKCS12")) {
                        _fipsKeystores.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }

                if (service.getType().equalsIgnoreCase("MessageDigest")) {
                    if (!provider.getName().equalsIgnoreCase("SUN") && !service.getAlgorithm().equalsIgnoreCase("MD5")) {
                        _fipsMessageDigests.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }

                if (service.getType().equalsIgnoreCase("SecretKeyFactory")) {
                    if (!provider.getName().equalsIgnoreCase("SunJCE")) {
                        _fipsSecretKeyFactories.put(service.getAlgorithm().toUpperCase(), provider.getName());
                    }
                }
            }

        }
        
        LOGGER.info("Found {} installed security providers: {}", _providerNames.size(), _providerNames);
        FipsManager.fipsKeystores = Collections.unmodifiableMap(_fipsKeystores);
        FipsManager.fipsMessageDigests = Collections.unmodifiableMap(_fipsMessageDigests);
        FipsManager.fipsSecretKeyFactories = Collections.unmodifiableMap(_fipsSecretKeyFactories);
        
        fipsKeyStoreType = evaluateDefaultKeyStoreType();
        Security.setProperty("keystore.type", fipsKeyStoreType);
        
        //https://stackoverflow.com/questions/54794570/using-pkixvalidator-with-bouncycastlefipsprovider-for-server-certificate-validat
        //Security.setProperty("ssl.KeyManagerFactory.algorithm", "PKIX");
        //Security.setProperty("ssl.TrustManagerFactory.algorithm", "PKIX");

        //https://developer.jboss.org/message/978202#978202
        
        //System.setProperty("javax.net.ssl.trustStoreType", fipsKeyStoreType);
        
/*
 * 
 
 /Library/Java/JavaVirtualMachines/jdk1.8.0_181.jdk/Contents/Home/bin/keytool  -importkeystore -srckeystore cacerts -destkeystore ~/cacerts.BCFKS -deststoretype BCFKS -destprovidername BCFIPS -v  -providerpath ../ext/bc-fips-1.0.1.jar -providerclass org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider -deststorepass changeit -srcstorepass changeit
 
 * 
 */
        
        //cacerts in fips (BCFKS) format
        System.setProperty("javax.net.ssl.trustStore", "/Users/salyh/sgdev/search-guard/src/test/resources/fips/cacerts.BCFKS");
        System.setProperty("javax.net.ssl.trustStorePassword", "changeit");



    }
    
    public static void main(String[] args) throws Exception {
        
        Policy.setPolicy(new Policy() {

            @Override
            public boolean implies(ProtectionDomain domain, Permission permission) {
                if(permission.getClass().getName().equals("org.bouncycastle.crypto.CryptoServicesPermission")) {
                    if(permission.getActions().equals("[unapprovedModeEnabled]")) {
                        System.out.println(permission);
                        return false;
                    }
                }
                return true;
            }
            
        });

        System.setSecurityManager(new SecurityManager());
        
        System.out.println(
                "Java Version: " + System.getProperty("java.version") + " " + System.getProperty("java.vendor"));
        System.out.println("JVM Impl.: " + System.getProperty("java.vm.version") + " "
                + System.getProperty("java.vm.vendor") + " " + System.getProperty("java.vm.name"));
        
        //String hashed = computePBKDF("myclear123upassword".toCharArray());
        //System.out.println();
        
        try {
           
           //final int aesMaxKeyLength = Cipher.getMaxAllowedKeyLength("AES");
           //System.out.println("Max AES key len: "+aesMaxKeyLength);
           
           //final int rsaMaxKeyLength = Cipher.getMaxAllowedKeyLength("RSA");
           //System.out.println("Max RSA key len: "+rsaMaxKeyLength);
           
           String md5Provider = MessageDigest.getInstance("RIPEMD128").getProvider().getClass().getName();
           System.out.println("md5Provider: "+ md5Provider);
           
           System.out.println(MessageDigest.getInstance("MD2").digest(new byte[] {1,2,3}));
           
           String sha1Provider = MessageDigest.getInstance("SHA-1").getProvider().getClass().getName();
           System.out.println("sha1Provider: "+ sha1Provider);
            
        } catch (final Throwable e) {
            e.printStackTrace();
        }
        
        Map<String, String> fipsKeystores = new HashMap<>();
        Map<String, String> fipsMessageDigest = new HashMap<>();
        Map<String, String> trustManagerFactory = new HashMap<>();
        
        Provider [] providerList = Security.getProviders();
        System.out.println("Security provider count: "+providerList.length);
        System.out.println();
        for (Provider provider : providerList)
         {
           System.out.println("Name: "  + provider.getName()+" (Version: "+provider.getVersion()+")");
           System.out.println("Class: " + provider.getClass());
           System.out.println("Information: " + provider.getInfo());

           Set<Service> serviceList = provider.getServices();
           for (Service service : serviceList)
            {
              System.out.println("    Service Type: " + service.getType() + " Algorithm " + service.getAlgorithm());
              
              if(service.getType().equalsIgnoreCase("KeyStore")) {
                  if(!provider.getName().equalsIgnoreCase("SUN") 
                          && !provider.getName().equalsIgnoreCase("SunJCE")
                          && !provider.getName().equalsIgnoreCase("Apple")
                          && !service.getAlgorithm().equalsIgnoreCase("PKCS12")) {
                      fipsKeystores.put(service.getAlgorithm(), provider.getName());
                  }
              }
              
              if(service.getType().equalsIgnoreCase("MessageDigest")) {
                  if(!provider.getName().equalsIgnoreCase("SUN") 
                          && !service.getAlgorithm().equalsIgnoreCase("MD5")) {
                      fipsMessageDigest.put(service.getAlgorithm(), provider.getName());
                  }
              }
              
              if(service.getType().equalsIgnoreCase("TrustManagerFactory")) {
            {
                      trustManagerFactory.put(service.getAlgorithm(), provider.getName());
                  }
              }
              
            }
           
           System.out.println();
           System.out.println("FIPS Keystores");
           for(Entry<String, String> ks:fipsKeystores.entrySet()) {
               System.out.println(ks.getKey()+"="+ks.getValue());
           }
           System.out.println();
           System.out.println("FIPS TrustManagerFactory");
           for(Entry<String, String> ks:trustManagerFactory.entrySet()) {
               System.out.println(ks.getKey()+"="+ks.getValue());
           }
           System.out.println();
           System.out.println("FIPS MessageDisgest");
           for(Entry<String, String> ks:fipsMessageDigest.entrySet()) {
               System.out.println(ks.getKey()+"="+ks.getValue());
           }
           
         }
    }

    private static SecretKey generatePasswordHash0(char[] plainTextPassword, byte[] salt, int rounds) {
        checkInitialized();
        
        
        
        try {
            
            SecretKeyFactory keyFact = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512", FipsManager.fipsSecretKeyFactories.get("PBKDF2WithHmacSHA512".toUpperCase())); 
            PBEKeySpec pbeSpec = new PBEKeySpec(plainTextPassword, salt, 12288, 128*8);
            return keyFact.generateSecret(pbeSpec);
            
            
            /*SecretKeyFactory keyFact = SecretKeyFactory.getInstance("HmacSHA384", "BCFIPS");
            SecretKey hmacKey = keyFact.generateSecret(new PBEKeySpec(plainTextPassword, salt, 1024, 256));
            return Base64.getEncoder().encodeToString(new SecretKeySpec(hmacKey.getEncoded(), "AES").getEncoded());*/
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String generatePasswordHash(char[] plainTextPassword, byte[] salt, int rounds) {
        checkInitialized();
        
        if(!isFipsEnabled()) {
            return OpenBSDBCrypt.generate(plainTextPassword, salt, 12);
        }
        
        try {
            
            SecretKey key = generatePasswordHash0(plainTextPassword, salt, rounds);
            return Base64.getEncoder().encodeToString(salt)+"#"+Base64.getEncoder().encodeToString(key.getEncoded());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static boolean checkPasswordHash(String hash, char[] plainTextPassword) {
        checkInitialized();
        if(hash == null || hash.isEmpty() 
                || plainTextPassword == null || plainTextPassword.length == 0) {
            return false;
        }
        
        if(!isFipsEnabled() && hash.startsWith("$")) {
            return OpenBSDBCrypt.checkPassword(hash, plainTextPassword);
        }
        
        //remove
        if(hash.startsWith("$")) {
            return OpenBSDBCrypt.checkPassword(hash, plainTextPassword);
        }
        
        final String[] splittedHash = hash.split("#");
        
        if(splittedHash.length != 2) {
            return false;
        }
        
        final SecretKey hashedPlaintext = generatePasswordHash0(plainTextPassword,Base64.getDecoder().decode(splittedHash[0]), 0);
        
        return MessageDigest.isEqual(Base64.getDecoder().decode(splittedHash[1]), hashedPlaintext.getEncoded());
    }

    public static String sha256Hash(byte[] in) throws NoSuchAlgorithmException, NoSuchProviderException {
        checkInitialized();
        return Hex.toHexString(hash(in, "SHA-256"));
    }

    public static byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException, NoSuchProviderException {
        checkInitialized();
        if(!isFipsEnabled()) {
            return MessageDigest.getInstance(algo).digest(in);
        }
        return MessageDigest.getInstance(algo, fipsMessageDigests.get(algo.toUpperCase())).digest(in);
    }
    
    //Blake2bDigest
    public static byte[] fastHash(byte[] in, byte[] defaultSalt) throws NoSuchAlgorithmException, NoSuchProviderException {
        checkInitialized();
        if(!isFipsEnabled()) {
            final Blake2bDigest hash = new Blake2bDigest(null, 32, null, defaultSalt);
            hash.update(in, 0, in.length);
            final byte[] out = new byte[hash.getDigestSize()];
            hash.doFinal(out, 0);
            return Hex.encode(out);
        }
        
        return hash(in, "SHA3-256");
    }

    public static boolean isInitialized() {
        return INITIALIZED.get();
    }
    
    public static boolean isFipsEnabled() {
        checkInitialized();
        return FIPS_ENABLED.get();
    }
    
    private static void checkInitialized() {
        if(!isInitialized()) {
            throw new RuntimeException("not yet initialized");
        }
    }

    public static void initialize(boolean fipsEnabled) {
       
        if(INITIALIZED.get()) {
            throw new RuntimeException("already initialized");
        }
        
        final SecurityManager sm = System.getSecurityManager();

        if (sm != null) {
            sm.checkPermission(new SpecialPermission());
        } else if (fipsEnabled){
            throw new RuntimeException("No security manager installed");
        }
        
        if(fipsEnabled) {
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    evaluateProviders();
                    checkSunJSSEProvider();
                    return null;
                }
            });
        } else {
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                public Object run() {
                    if (Security.getProvider("BC") == null) {
                        
                        try {
                            Class.forName("org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider");
                            throw new RuntimeException("Found BouncyCastleFipsProvider in classpath but we are not in FIPS mode");
                        } catch (ClassNotFoundException e) {
                            //ignore
                        }
                        
                        
                        Security.addProvider(new BouncyCastleProvider());
                    } else {
                        throw new RuntimeException("BC security provider already added");
                    }
                    return null;
                }
            });
        }
        
        FIPS_ENABLED.set(fipsEnabled);
        INITIALIZED.set(true);
        
        LOGGER.info("Initialized with fips {}", isFipsEnabled()?"enabled":"disabled");
    }
    
    private static void checkSunJSSEProvider() {
        if (Security.getProvider("SunJSSE") == null) {
            LOGGER.info("No SunJSSE provider found");
        } else {
            
            LOGGER.info(Security.getProvider("SunJSSE").getInfo());
            
            if(!Security.getProvider("SunJSSE").getInfo().contains("Sun JSSE provider (FIPS mode, crypto provider")) {
                throw new RuntimeException("SunJSSE not in FIPS mode");
            }
            
            System.out.println("keystore.type "+ Security.getProperty("keystore.type"));
        }
    }
    
    private static String evaluateDefaultKeyStoreType() {
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
    
    private static String evaluateKeyStoreTypeFromDetectedTypes() {
        
        if(FipsManager.fipsKeystores.isEmpty()) {
            throw new RuntimeException("No FIPS compatible keystore type found");
        } else if(fipsKeystores.size() > 1) {
            final String kstype = FipsManager.fipsKeystores.keySet().iterator().next();
            LOGGER.warn("Multiple FIPS compatible keystore types found ({}). It's recommended to set keystore.type in your java security file. "
                    + "Will use: {}", FipsManager.fipsKeystores, kstype);
            return kstype;
        } else {
            final String kstype = FipsManager.fipsKeystores.keySet().iterator().next();
            LOGGER.info("Use FIPS compatible keystore type '{}' as default", kstype);
            return kstype;
        }
    }
}
