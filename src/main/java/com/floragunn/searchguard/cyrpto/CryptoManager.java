package com.floragunn.searchguard.cyrpto;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

public interface CryptoManager {
    
    KeyStore getKeystoreInstance(String keystoreType) throws KeyStoreException, NoSuchProviderException;
    KeyManager[] getKeyManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException;
    TrustManager[] getTrustManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException;
    String generatePasswordHash(char[] plainTextPassword, byte[] salt, int rounds);
    boolean checkPasswordHash(String hash, char[] plainTextPassword);
    byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException, NoSuchProviderException;
    byte[] fastHash(byte[] in, byte[] defaultSalt) throws NoSuchAlgorithmException, NoSuchProviderException;
    String validateLicense(String in) throws Exception;
    void checkTlsProtocols(List<String> tlsProtocols);
    void checkTlsChipers(List<String> tlsChipers);
    boolean isFipsEnabled();
    boolean isInitialized();

}
