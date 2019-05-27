package com.floragunn.searchguard.crypto;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivilegedAction;
import java.security.Security;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.bouncycastle.bcpg.ArmoredInputStream;
import org.bouncycastle.crypto.digests.Blake2bDigest;
import org.bouncycastle.crypto.generators.OpenBSDBCrypt;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSignature;
import org.bouncycastle.openpgp.PGPSignatureList;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.bc.BcKeyFingerprintCalculator;
import org.bouncycastle.openpgp.operator.bc.BcPGPContentVerifierBuilderProvider;
import org.bouncycastle.util.encoders.Hex;
import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.support.LicenseHelper;
import com.floragunn.searchguard.support.SgUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.io.BaseEncoding;

import io.netty.handler.ssl.OpenSsl;

final class DefaultCryptoManager extends AbstractCryptoManager {
    
    
    private static final List<String> DEFAULT_SECURE_TLS_CHIPERS;
    
    // @formatter:off
    private static final List<String> SECURE_TLS_CHIPERS  = ImmutableList.of(
        //TLS_<key exchange and authentication algorithms>_WITH_<bulk cipher and message authentication algorithms>
        
        //Example (including unsafe ones)
        //Protocol: TLS, SSL
        //Key Exchange    RSA, Diffie-Hellman, ECDH, SRP, PSK
        //Authentication  RSA, DSA, ECDSA
        //Bulk Ciphers    RC4, 3DES, AES
        //Message Authentication  HMAC-SHA256, HMAC-SHA1, HMAC-MD5
        

        //thats what chrome 48 supports (https://cc.dcsec.uni-hannover.de/)
        //(c0,2b)ECDHE-ECDSA-AES128-GCM-SHA256128 BitKey exchange: ECDH, encryption: AES, MAC: SHA256.
        //(c0,2f)ECDHE-RSA-AES128-GCM-SHA256128 BitKey exchange: ECDH, encryption: AES, MAC: SHA256.
        //(00,9e)DHE-RSA-AES128-GCM-SHA256128 BitKey exchange: DH, encryption: AES, MAC: SHA256.
        //(cc,14)ECDHE-ECDSA-CHACHA20-POLY1305-SHA256128 BitKey exchange: ECDH, encryption: ChaCha20 Poly1305, MAC: SHA256.
        //(cc,13)ECDHE-RSA-CHACHA20-POLY1305-SHA256128 BitKey exchange: ECDH, encryption: ChaCha20 Poly1305, MAC: SHA256.
        //(c0,0a)ECDHE-ECDSA-AES256-SHA256 BitKey exchange: ECDH, encryption: AES, MAC: SHA1.
        //(c0,14)ECDHE-RSA-AES256-SHA256 BitKey exchange: ECDH, encryption: AES, MAC: SHA1.
        //(00,39)DHE-RSA-AES256-SHA256 BitKey exchange: DH, encryption: AES, MAC: SHA1.
        //(c0,09)ECDHE-ECDSA-AES128-SHA128 BitKey exchange: ECDH, encryption: AES, MAC: SHA1.
        //(c0,13)ECDHE-RSA-AES128-SHA128 BitKey exchange: ECDH, encryption: AES, MAC: SHA1.
        //(00,33)DHE-RSA-AES128-SHA128 BitKey exchange: DH, encryption: AES, MAC: SHA1.
        //(00,9c)RSA-AES128-GCM-SHA256128 BitKey exchange: RSA, encryption: AES, MAC: SHA256.
        //(00,35)RSA-AES256-SHA256 BitKey exchange: RSA, encryption: AES, MAC: SHA1.
        //(00,2f)RSA-AES128-SHA128 BitKey exchange: RSA, encryption: AES, MAC: SHA1.
        //(00,0a)RSA-3DES-EDE-SHA168 BitKey exchange: RSA, encryption: 3DES, MAC: SHA1.
        
        //thats what firefox 42 supports (https://cc.dcsec.uni-hannover.de/)
        //(c0,2b) ECDHE-ECDSA-AES128-GCM-SHA256
        //(c0,2f) ECDHE-RSA-AES128-GCM-SHA256
        //(c0,0a) ECDHE-ECDSA-AES256-SHA
        //(c0,09) ECDHE-ECDSA-AES128-SHA
        //(c0,13) ECDHE-RSA-AES128-SHA
        //(c0,14) ECDHE-RSA-AES256-SHA
        //(00,33) DHE-RSA-AES128-SHA
        //(00,39) DHE-RSA-AES256-SHA
        //(00,2f) RSA-AES128-SHA
        //(00,35) RSA-AES256-SHA
        //(00,0a) RSA-3DES-EDE-SHA

        //Mozilla modern browsers
        //https://wiki.mozilla.org/Security/Server_Side_TLS
        "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "TLS_DHE_DSS_WITH_AES_128_GCM_SHA256",
        "TLS_DHE_DSS_WITH_AES_256_GCM_SHA384",
        "TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
        "TLS_DHE_DSS_WITH_AES_128_CBC_SHA256",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "TLS_DHE_DSS_WITH_AES_256_CBC_SHA",
        "TLS_DHE_RSA_WITH_AES_256_CBC_SHA",
        
        //TLS 1.3 Java
        "TLS_AES_128_GCM_SHA256",
        "TLS_AES_256_GCM_SHA384",
        
        //TLS 1.3 OpenSSL
        "TLS_CHACHA20_POLY1305_SHA256",
        "TLS_AES_128_CCM_8_SHA256",
        "TLS_AES_128_CCM_SHA256",
        
        //IBM
        "SSL_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
        "SSL_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
        "SSL_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
        "SSL_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        "SSL_DHE_RSA_WITH_AES_128_GCM_SHA256",
        "SSL_DHE_DSS_WITH_AES_128_GCM_SHA256",
        "SSL_DHE_DSS_WITH_AES_256_GCM_SHA384",
        "SSL_DHE_RSA_WITH_AES_256_GCM_SHA384",
        "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
        "SSL_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
        "SSL_ECDHE_RSA_WITH_AES_128_CBC_SHA",
        "SSL_ECDHE_ECDSA_WITH_AES_128_CBC_SHA",
        "SSL_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        "SSL_ECDHE_ECDSA_WITH_AES_256_CBC_SHA384",
        "SSL_ECDHE_RSA_WITH_AES_256_CBC_SHA",
        "SSL_ECDHE_ECDSA_WITH_AES_256_CBC_SHA",
        "SSL_DHE_RSA_WITH_AES_128_CBC_SHA256",
        "SSL_DHE_RSA_WITH_AES_128_CBC_SHA",
        "SSL_DHE_DSS_WITH_AES_128_CBC_SHA256",
        "SSL_DHE_RSA_WITH_AES_256_CBC_SHA256",
        "SSL_DHE_DSS_WITH_AES_256_CBC_SHA",
        "SSL_DHE_RSA_WITH_AES_256_CBC_SHA"
        
        //some others
        //"TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384",
        //"TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384",
        //"TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384", 
        //"TLS_DHE_RSA_WITH_AES_256_CBC_SHA", 
        //"TLS_DHE_RSA_WITH_AES_256_GCM_SHA384",
        //"TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256", 
        //"TLS_DHE_RSA_WITH_AES_128_GCM_SHA256",
        //"TLS_DHE_RSA_WITH_AES_128_CBC_SHA",
        //"TLS_RSA_WITH_AES_128_CBC_SHA256",
        //"TLS_RSA_WITH_AES_128_GCM_SHA256",
        //"TLS_RSA_WITH_AES_128_CBC_SHA",
        //"TLS_RSA_WITH_AES_256_CBC_SHA",
        );
        // @formatter:on
    
    static {
        List<String> jdkSupportedCiphers = new ArrayList<String>(CryptoManagerFactory.JDK_SUPPORTED_SSL_CIPHERS);
        jdkSupportedCiphers.retainAll(SECURE_TLS_CHIPERS);
        DEFAULT_SECURE_TLS_CHIPERS = Collections.unmodifiableList(jdkSupportedCiphers);
    }

    public DefaultCryptoManager() {
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
                    LOGGER.error("BC security provider already added (not a fatal error but this should not happen normally");
                    //RuntimeException e = new RuntimeException("BC security provider already added");
                    //e.printStackTrace();
                    //throw e;
                }
                return null;
            }
        });
    }

    @Override
    public KeyStore getKeystoreInstance(String keystoreType) throws KeyStoreException, NoSuchProviderException {
        return KeyStore.getInstance(keystoreType);
    }

    @Override
    public String generatePasswordHash(char[] plainTextPassword) {
        
        if(plainTextPassword == null || plainTextPassword.length == 0) {
            throw new IllegalArgumentException("plainTextPassword can to be null or empty");
        }
        
        try {
            final byte[] salt = salt(16);
            return OpenBSDBCrypt.generate(plainTextPassword, salt, 12);
        } finally {
            Arrays.fill(plainTextPassword, '\0');
        }
    }

    @Override
    public boolean checkPasswordHash(String hash, char[] plainTextPassword) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        
        if(plainTextPassword == null || plainTextPassword.length == 0) {
            throw new IllegalArgumentException("plainTextPassword can to be null or empty");
        }

        if(hash.charAt(0) != '$') {
            return checkPBKDF2PasswordHash(hash, plainTextPassword, null);
        }
        
        try {
            return OpenBSDBCrypt.checkPassword(hash, plainTextPassword);
        } finally {
            Arrays.fill(plainTextPassword, '\0');
        }
    }

    @Override
    public byte[] hash(byte[] in, String algo) throws NoSuchAlgorithmException, NoSuchProviderException {
        return MessageDigest.getInstance(algo).digest(in);
    }

    @Override
    public byte[] fastHash(byte[] in, byte[] defaultSalt) throws NoSuchAlgorithmException, NoSuchProviderException {
        final Blake2bDigest hash = new Blake2bDigest(null, 32, null, defaultSalt);
        hash.update(in, 0, in.length);
        final byte[] out = new byte[hash.getDigestSize()];
        hash.doFinal(out, 0);
        return out;
    }

    @Override
    public String validateLicense(String licenseText) throws Exception {
        return SgUtils.substringBetween(PGPLicenseHelper.validateLicense(licenseText), "{","}", true);
    }

    @Override
    public void checkTlsProtocols(List<String> tlsProtocols) {
        //noop
    }

    @Override
    public void checkTlsChipers(List<String> tlsChipers) {
        //noop
    }
    
    @Override
    public void validateSettings(Settings settings) {
        //noop
    }
    
    @Override
    public List<String> getDefaultTlsCiphers() {
        return DefaultCryptoManager.DEFAULT_SECURE_TLS_CHIPERS;
    }
    
    @Override
    public List<String> getDefaultTlsProtocols() {
        return AbstractCryptoManager.SECURE_TLS_PROTOCOLS;
    }
    
    @Override
    public boolean isHttpsClientCertRevocationSuported() {
        return true;
    }

    @Override
    public boolean isOpenSslAvailable() {
        return OpenSsl.isAvailable();
    }

    private static class PGPLicenseHelper {

        /**
         * Validate pgp signature of license
         * 
         * @param licenseText base64 encoded pgp signed license
         * @return The plain license in json (if validation is successful)
         * @throws PGPException if validation fails
         */
        public static String validateLicense(String licenseText) throws PGPException {
            
            licenseText = licenseText.trim().replaceAll("\\r|\\n", "");
            licenseText = licenseText.replace("---- SCHNIPP (Armored PGP signed JSON as base64) ----","");
            licenseText = licenseText.replace("---- SCHNAPP ----","");
            
            try {
                final byte[] armoredPgp = BaseEncoding.base64().decode(licenseText);

                final ArmoredInputStream in = new ArmoredInputStream(new ByteArrayInputStream(armoredPgp));

                //
                // read the input, making sure we ignore the last newline.
                //
                // https://github.com/bcgit/bc-java/blob/master/pg/src/test/java/org/bouncycastle/openpgp/test/PGPClearSignedSignatureTest.java

                final ByteArrayOutputStream bout = new ByteArrayOutputStream();
                int ch;

                while ((ch = in.read()) >= 0 && in.isClearText()) {
                    bout.write((byte) ch);
                }

                final KeyFingerPrintCalculator c = new BcKeyFingerprintCalculator();

                final PGPObjectFactory factory = new PGPObjectFactory(in, c);
                final PGPSignatureList sigL = (PGPSignatureList) factory.nextObject();
                final PGPPublicKeyRingCollection pgpRings = new PGPPublicKeyRingCollection(new ArmoredInputStream(
                        LicenseHelper.class.getResourceAsStream("/KEYS")), c);

                if (sigL == null || pgpRings == null || sigL.size() == 0 || pgpRings.size() == 0) {
                    throw new PGPException("Cannot find license signature");
                }

                final PGPSignature sig = sigL.get(0);
                final PGPPublicKey publicKey = pgpRings.getPublicKey(sig.getKeyID());

                if (publicKey == null || sig == null) {
                    throw new PGPException("license signature key mismatch");
                }

                sig.init(new BcPGPContentVerifierBuilderProvider(), publicKey);

                final ByteArrayOutputStream lineOut = new ByteArrayOutputStream();
                final InputStream sigIn = new ByteArrayInputStream(bout.toByteArray());
                int lookAhead = readInputLine(lineOut, sigIn);

                processLine(sig, lineOut.toByteArray());

                if (lookAhead != -1) {
                    do {
                        lookAhead = readInputLine(lineOut, lookAhead, sigIn);

                        sig.update((byte) '\r');
                        sig.update((byte) '\n');

                        processLine(sig, lineOut.toByteArray());
                    } while (lookAhead != -1);
                }

                if (!sig.verify()) {
                    throw new PGPException("Invalid license signature");
                }

                return bout.toString();
            } catch (final Exception e) {
                throw new PGPException(e.toString(), e);
            }
        }

        private static int readInputLine(final ByteArrayOutputStream bOut, final InputStream fIn) throws IOException {
            bOut.reset();

            int lookAhead = -1;
            int ch;

            while ((ch = fIn.read()) >= 0) {
                bOut.write(ch);
                if (ch == '\r' || ch == '\n') {
                    lookAhead = readPassedEOL(bOut, ch, fIn);
                    break;
                }
            }

            return lookAhead;
        }

        private static int readInputLine(final ByteArrayOutputStream bOut, int lookAhead, final InputStream fIn) throws IOException {
            bOut.reset();

            int ch = lookAhead;

            do {
                bOut.write(ch);
                if (ch == '\r' || ch == '\n') {
                    lookAhead = readPassedEOL(bOut, ch, fIn);
                    break;
                }
            } while ((ch = fIn.read()) >= 0);

            return lookAhead;
        }

        private static int readPassedEOL(final ByteArrayOutputStream bOut, final int lastCh, final InputStream fIn) throws IOException {
            int lookAhead = fIn.read();

            if (lastCh == '\r' && lookAhead == '\n') {
                bOut.write(lookAhead);
                lookAhead = fIn.read();
            }

            return lookAhead;
        }

        private static void processLine(final PGPSignature sig, final byte[] line) throws SignatureException, IOException {
            final int length = getLengthWithoutWhiteSpace(line);
            if (length > 0) {
                sig.update(line, 0, length);
            }
        }

        private static int getLengthWithoutWhiteSpace(final byte[] line) {
            int end = line.length - 1;

            while (end >= 0 && isWhiteSpace(line[end])) {
                end--;
            }

            return end + 1;
        }

        private static boolean isWhiteSpace(final byte b) {
            return b == '\r' || b == '\n' || b == '\t' || b == ' ';
        }
    }
}
