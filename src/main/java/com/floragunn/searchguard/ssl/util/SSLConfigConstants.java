/*
 * Copyright 2015-2017 floragunn GmbH
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 */

package com.floragunn.searchguard.ssl.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.elasticsearch.common.settings.Settings;

import com.floragunn.searchguard.crypto.CryptoManagerFactory;

public final class SSLConfigConstants {

    public static final String SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE = "searchguard.ssl.http.enable_openssl_if_available";
    public static final String SEARCHGUARD_SSL_HTTP_ENABLED = "searchguard.ssl.http.enabled";
    public static final boolean SEARCHGUARD_SSL_HTTP_ENABLED_DEFAULT = false;
    public static final String SEARCHGUARD_SSL_HTTP_CLIENTAUTH_MODE = "searchguard.ssl.http.clientauth_mode";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_ALIAS = "searchguard.ssl.http.keystore_alias";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_FILEPATH = "searchguard.ssl.http.keystore_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_PEMKEY_FILEPATH = "searchguard.ssl.http.pemkey_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_PEMKEY_PASSWORD = "searchguard.ssl.http.pemkey_password";
    public static final String SEARCHGUARD_SSL_HTTP_PEMCERT_FILEPATH = "searchguard.ssl.http.pemcert_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_PEMTRUSTEDCAS_FILEPATH = "searchguard.ssl.http.pemtrustedcas_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_PASSWORD = "searchguard.ssl.http.keystore_password";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_KEYPASSWORD = "searchguard.ssl.http.keystore_keypassword";
    public static final String SEARCHGUARD_SSL_HTTP_KEYSTORE_TYPE = "searchguard.ssl.http.keystore_type";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_ALIAS = "searchguard.ssl.http.truststore_alias";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_FILEPATH = "searchguard.ssl.http.truststore_filepath";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_PASSWORD = "searchguard.ssl.http.truststore_password";
    public static final String SEARCHGUARD_SSL_HTTP_TRUSTSTORE_TYPE = "searchguard.ssl.http.truststore_type";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE = "searchguard.ssl.transport.enable_openssl_if_available";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLED = "searchguard.ssl.transport.enabled";
    public static final boolean SEARCHGUARD_SSL_TRANSPORT_ENABLED_DEFAULT = true;
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION = "searchguard.ssl.transport.enforce_hostname_verification";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENFORCE_HOSTNAME_VERIFICATION_RESOLVE_HOST_NAME = "searchguard.ssl.transport.resolve_hostname";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_ALIAS = "searchguard.ssl.transport.keystore_alias";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_FILEPATH = "searchguard.ssl.transport.keystore_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMKEY_FILEPATH = "searchguard.ssl.transport.pemkey_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMKEY_PASSWORD = "searchguard.ssl.transport.pemkey_password";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMCERT_FILEPATH = "searchguard.ssl.transport.pemcert_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PEMTRUSTEDCAS_FILEPATH = "searchguard.ssl.transport.pemtrustedcas_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_PASSWORD = "searchguard.ssl.transport.keystore_password";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_KEYPASSWORD = "searchguard.ssl.transport.keystore_keypassword";
    public static final String SEARCHGUARD_SSL_TRANSPORT_KEYSTORE_TYPE = "searchguard.ssl.transport.keystore_type";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_ALIAS = "searchguard.ssl.transport.truststore_alias";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_FILEPATH = "searchguard.ssl.transport.truststore_filepath";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_PASSWORD = "searchguard.ssl.transport.truststore_password";
    public static final String SEARCHGUARD_SSL_TRANSPORT_TRUSTSTORE_TYPE = "searchguard.ssl.transport.truststore_type";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS = "searchguard.ssl.transport.enabled_ciphers";
    public static final String SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS = "searchguard.ssl.transport.enabled_protocols";
    public static final String SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS = "searchguard.ssl.http.enabled_ciphers";
    public static final String SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS = "searchguard.ssl.http.enabled_protocols";
    public static final String SEARCHGUARD_SSL_CLIENT_EXTERNAL_CONTEXT_ID = "searchguard.ssl.client.external_context_id";
    public static final String SEARCHGUARD_SSL_TRANSPORT_PRINCIPAL_EXTRACTOR_CLASS = "searchguard.ssl.transport.principal_extractor_class";

    public static final String SEARCHGUARD_SSL_HTTP_CRL_FILE = "searchguard.ssl.http.crl.file_path";
    public static final String SEARCHGUARD_SSL_HTTP_CRL_VALIDATE = "searchguard.ssl.http.crl.validate";
    public static final String SEARCHGUARD_SSL_HTTP_CRL_PREFER_CRLFILE_OVER_OCSP = "searchguard.ssl.http.crl.prefer_crlfile_over_ocsp";
    public static final String SEARCHGUARD_SSL_HTTP_CRL_CHECK_ONLY_END_ENTITIES = "searchguard.ssl.http.crl.check_only_end_entities";    
    public static final String SEARCHGUARD_SSL_HTTP_CRL_DISABLE_OCSP = "searchguard.ssl.http.crl.disable_ocsp";    
    public static final String SEARCHGUARD_SSL_HTTP_CRL_DISABLE_CRLDP = "searchguard.ssl.http.crl.disable_crldp";   
    public static final String SEARCHGUARD_SSL_HTTP_CRL_VALIDATION_DATE = "searchguard.ssl.http.crl.validation_date";

    public static final String SEARCHGUARD_SSL_ALLOW_CLIENT_INITIATED_RENEGOTIATION = "searchguard.ssl.allow_client_initiated_renegotiation";

    public static final String DEFAULT_STORE_PASSWORD = "changeit"; //#16
    
    public static final String JDK_TLS_REJECT_CLIENT_INITIATED_RENEGOTIATION = "jdk.tls.rejectClientInitiatedRenegotiation";
    
    public static final String[] getSecureSSLProtocols(Settings settings, boolean http)
    {
        List<String> configuredProtocols = null;
        
        if(settings != null) {
            if(http) {
                configuredProtocols = settings.getAsList(SEARCHGUARD_SSL_HTTP_ENABLED_PROTOCOLS, Collections.emptyList());
            } else {
                configuredProtocols = settings.getAsList(SEARCHGUARD_SSL_TRANSPORT_ENABLED_PROTOCOLS, Collections.emptyList());
            }
        }
        
        if(configuredProtocols == null || configuredProtocols.isEmpty()) {
            configuredProtocols = CryptoManagerFactory.getInstance().getDefaultTlsProtocols();
        }
        
        CryptoManagerFactory.getInstance().checkTlsProtocols(configuredProtocols);
        return configuredProtocols.toArray(new String[0]);
    }
    
    
    public static final List<String> getSecureSSLCiphers(Settings settings, boolean http) {
        
        List<String> configuredCiphers = null;
        
        if(settings != null) {
            if(http) {
                configuredCiphers = settings.getAsList(SEARCHGUARD_SSL_HTTP_ENABLED_CIPHERS, Collections.emptyList());
            } else {
                configuredCiphers = settings.getAsList(SEARCHGUARD_SSL_TRANSPORT_ENABLED_CIPHERS, Collections.emptyList());
            }
        }
        
        if(configuredCiphers == null || configuredCiphers.isEmpty()) {
            configuredCiphers = CryptoManagerFactory.getInstance().getDefaultTlsCiphers();
        }

        CryptoManagerFactory.getInstance().checkTlsChipers(configuredCiphers);
        return Collections.unmodifiableList(configuredCiphers);

        
    }
    
    private SSLConfigConstants() {

    }

}
