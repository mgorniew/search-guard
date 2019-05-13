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

public final class FipsCryptoManager extends AbstractCryptoManager {

    @Override
    public KeyStore getKeystoreInstance(String keystoreType) throws KeyStoreException, NoSuchProviderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public KeyManager[] getKeyManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException,
            NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TrustManager[] getTrustManagers(String keyStoreFile, char[] password) throws NoSuchAlgorithmException, KeyStoreException,
            NoSuchProviderException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String generatePasswordHash(char[] plainTextPassword, byte[] salt, int rounds) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean checkPasswordHash(String hash, char[] plainTextPassword) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException, NoSuchProviderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public byte[] fastHash(byte[] in, byte[] defaultSalt) throws NoSuchAlgorithmException, NoSuchProviderException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String validateLicense(String in) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void checkTlsProtocols(List<String> tlsProtocols) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void checkTlsChipers(List<String> tlsChipers) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public boolean isFipsEnabled() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isInitialized() {
        // TODO Auto-generated method stub
        return false;
    }

}
