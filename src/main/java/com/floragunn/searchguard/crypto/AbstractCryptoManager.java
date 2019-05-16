package com.floragunn.searchguard.crypto;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Chars;

public abstract class AbstractCryptoManager implements CryptoManager {
    
    protected final Logger LOGGER = LogManager.getLogger(this.getClass());
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    protected static final List<String> SECURE_TLS_PROTOCOLS  = ImmutableList.of("TLSv1.3", "TLSv1.2", "TLSv1.1");
    
    
    private static final boolean ALLOW_SHORT_PASSWORDS = true;
    
    @Override
    public KeyManager[] getKeyManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = getKeystoreInstance(KeyStore.getDefaultType());
        ks.load(new FileInputStream(keyStoreFile), password);
        kmf.init(ks, password);
        return kmf.getKeyManagers();
    }
    
    @Override
    public TrustManager[] getTrustManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException {
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        KeyStore ks = getKeystoreInstance(KeyStore.getDefaultType());
        ks.load(new FileInputStream(keyStoreFile), password);
        tmf.init(ks);
        return tmf.getTrustManagers();
    }
    
    @Override
    public final String generatePBKDF2PasswordHash(char[] plainTextPassword, String provider) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        final byte[] salt = salt(32);
        SecretKey key = generatePBKDF2Hash(plainTextPassword, salt, 10000, provider);
        return Base64.getEncoder().encodeToString(salt)+"#"+Base64.getEncoder().encodeToString(key.getEncoded());
    }

    protected final boolean checkPBKDF2PasswordHash(String hash, char[] plainTextPassword, String provider) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        try {
            if(hash == null || hash.isEmpty() 
                    || plainTextPassword == null || plainTextPassword.length == 0) {
                return false;
            }
            
            //In fips mode paswords must be at least 112 bits
            if(ALLOW_SHORT_PASSWORDS && plainTextPassword.length < 14) {
                final char[] hashedShortPwd = Hex.encodeHex(hash(new String(plainTextPassword).getBytes(StandardCharsets.UTF_8), "SHA-256"));
                plainTextPassword = Chars.concat(plainTextPassword, hashedShortPwd);
            }
            
            final String[] splittedHash = hash.split("#");
            
            if(splittedHash.length != 2) {
                return false;
            }
            
            final SecretKey hashedPlaintext = generatePBKDF2Hash(plainTextPassword,Base64.getDecoder().decode(splittedHash[0]), 10000, provider);
            return MessageDigest.isEqual(Base64.getDecoder().decode(splittedHash[1]), hashedPlaintext.getEncoded());
        } finally {
            Arrays.fill(plainTextPassword, '\0');
        }
    }
    
    protected final SecretKey generatePBKDF2Hash(char[] plainTextPassword, final byte[] salt, final int iterationCount, final String provider) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        try {
            if(plainTextPassword == null || plainTextPassword.length == 0) {
                throw new IllegalArgumentException("plainTextPassword can to be null or empty");
            }
            
            if(salt == null || salt.length < 32) {
                throw new IllegalArgumentException("salt can to be null and must have 32 byte or more");
            }
            
            if(iterationCount < 10000) {
                throw new IllegalArgumentException("iterationCount must 10000 or more");
            }
            
            //In fips mode paswords must be at least 112 bits
            if(ALLOW_SHORT_PASSWORDS && plainTextPassword.length < 14) {
                final char[] hashedShortPwd = Hex.encodeHex(hash(new String(plainTextPassword).getBytes(StandardCharsets.UTF_8), "SHA-256"));
                plainTextPassword = Chars.concat(plainTextPassword, hashedShortPwd);
            }
            
            
            SecretKeyFactory keyFact;
            
            if(provider == null) {
                keyFact = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512");
            } else {
                keyFact = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512", provider);
            }
            PBEKeySpec pbeSpec = new PBEKeySpec(plainTextPassword, salt, iterationCount, 256); //256 bit key len (= 32 byte)
            return keyFact.generateSecret(pbeSpec);
        } finally {
            Arrays.fill(plainTextPassword, '\0');
        }
    }
    
    protected static byte[] salt(int lengthInBytes) {
        byte[] salt = new byte[lengthInBytes];
        SECURE_RANDOM.nextBytes(salt);
        return salt;
    }
}
