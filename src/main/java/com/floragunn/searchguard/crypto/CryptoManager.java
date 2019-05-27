package com.floragunn.searchguard.crypto;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

import org.elasticsearch.common.settings.Settings;

public interface CryptoManager {
    
    KeyStore getKeystoreInstance(String keystoreType) throws KeyStoreException, NoSuchProviderException;
    KeyManager[] getKeyManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException;
    TrustManager[] getTrustManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException, NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException;
    String generatePasswordHash(char[] plainTextPassword) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException;
    String generatePBKDF2PasswordHash(char[] plainTextPassword, String provider) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException;
    boolean checkPasswordHash(String hash, char[] plainTextPassword) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException;
    byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException, NoSuchProviderException;
    byte[] fastHash(byte[] in, byte[] defaultSalt) throws NoSuchAlgorithmException, NoSuchProviderException;
    String validateLicense(String in) throws Exception;
    void checkTlsProtocols(List<String> tlsProtocols);
    void checkTlsChipers(List<String> tlsChipers);
    //boolean isFipsEnabled();
    void validateSettings(Settings settings);
    List<String> getDefaultTlsCiphers();
    List<String> getDefaultTlsProtocols();
    boolean isHttpsClientCertRevocationSuported();
    boolean isOpenSslAvailable();
}
