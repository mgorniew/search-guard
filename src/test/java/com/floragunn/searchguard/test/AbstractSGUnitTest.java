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

package com.floragunn.searchguard.test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.security.Security;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Netty4Plugin;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;

import com.floragunn.searchguard.SearchGuardPlugin;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateAction;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateRequest;
import com.floragunn.searchguard.action.configupdate.ConfigUpdateResponse;
import com.floragunn.searchguard.crypto.CryptoManagerFactory;
import com.floragunn.searchguard.sgconf.impl.CType;
import com.floragunn.searchguard.ssl.util.SSLConfigConstants;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.test.helper.cluster.ClusterInfo;
import com.floragunn.searchguard.test.helper.file.FileHelper;
import com.floragunn.searchguard.test.helper.rest.RestHelper.HttpResponse;
import com.floragunn.searchguard.test.helper.rules.SGTestWatcher;
import com.google.common.base.Strings;

import io.netty.handler.ssl.OpenSsl;

public abstract class AbstractSGUnitTest {
    
    protected static final AtomicLong num = new AtomicLong();
    protected static boolean withRemoteCluster;

	static {
	    
	    System.setProperty("sg.no_enforce_crypto_manager_init", "true");
	    
	    if(System.getSecurityManager() == null) {
    	    //we need a security in case we test with FIPS
    	    Policy.setPolicy(new Policy() {
    
                @Override
                public boolean implies(ProtectionDomain domain, Permission permission) {
                    if(permission.getClass().getName().equals("org.bouncycastle.crypto.CryptoServicesPermission")) {
                        if(permission.getActions().equals("[unapprovedModeEnabled]")) {
                            //System.out.println(permission);
                            return false;
                        }
                    }
                    return true;
                }
                
            });
    
            System.setSecurityManager(new SecurityManager());
            System.out.println("Security Manager installed (Unittests)");
	    } else {
	        System.out.println("Security Manager for Unittests already installed)");
	    }
	    
	    CryptoManagerFactory.initialize(Security.getProvider("SunJSSE").getInfo().contains("Sun JSSE provider (FIPS mode, crypto provider"));

	    System.out.println("UT FIPS: "+utFips()); //initialize cryptomanager
	    
		System.out.println("OS: " + System.getProperty("os.name") + " " + System.getProperty("os.arch") + " "
				+ System.getProperty("os.version"));
		System.out.println(
				"Java Version: " + System.getProperty("java.version") + " " + System.getProperty("java.vendor"));
		System.out.println("JVM Impl.: " + System.getProperty("java.vm.version") + " "
				+ System.getProperty("java.vm.vendor") + " " + System.getProperty("java.vm.name"));
		System.out.println("Free memory: " + new ByteSizeValue(Runtime.getRuntime().freeMemory()));
		System.out.println("Max memory: " + new ByteSizeValue(Runtime.getRuntime().maxMemory()));
		System.out.println("Total memory: " + new ByteSizeValue(Runtime.getRuntime().totalMemory()));
		System.out.println(org.elasticsearch.common.Strings.toString(JvmInfo.jvmInfo(), true, true));
		System.out.println("Open SSL loadable: " + OpenSsl.isAvailable());
		System.out.println("Open SSL available: " + CryptoManagerFactory.getInstance().isOpenSslAvailable());
		System.out.println("Open SSL version: " + OpenSsl.versionString());
		System.out.println("JNDI hostname validation enabled by default: "+SgUtils.isJndiHostnameValidationEnabledByDefault());
		withRemoteCluster = Boolean.parseBoolean(System.getenv("TESTARG_unittests_with_remote_cluster"));
		System.out.println("With remote cluster: " + withRemoteCluster);
	}
	
	protected final Logger log = LogManager.getLogger(this.getClass());
    public static final ThreadPool MOCK_POOL = new ThreadPool(Settings.builder().put("node.name",  "mock").build());
	
    //TODO Test Matrix
    protected boolean allowOpenSSL = false; //disabled, we test this already in SSL Plugin
    //enable//disable enterprise modules
    //1node and 3 node
    
	@Rule
	public TestName name = new TestName();
	
	@Rule
    public final TemporaryFolder repositoryPath = new TemporaryFolder();

	@Rule
	public final TestWatcher testWatcher = new SGTestWatcher();
	
	static Set<String> tmp = new HashSet<>();
	static StringBuffer out = new StringBuffer();
	
	public static Header encodeBasicHeader(final String username, final String password) {
	    
	    if(!tmp.contains(username+password)) {
	        tmp.add(username+password);
	        try {
                out.append("\n"+username+":\n  #password: "+password+"\n  hash: '"+CryptoManagerFactory.getInstance().generatePBKDF2PasswordHash(password.toCharArray(), null)+"'");
            
	        

                FileUtils.write(new File("/tmp/migrated.yml"), out.toString(), StandardCharsets.UTF_8, false);

	        } catch (Throwable e) {
                e.printStackTrace();
                
            }
	        
	        
	    }
	    
		return new BasicHeader("Authorization", "Basic "+Base64.encodeBase64String(
				(username + ":" + Objects.requireNonNull(password)).getBytes(StandardCharsets.UTF_8)));
	}
	
	protected static class TransportClientImpl extends TransportClient {

        public TransportClientImpl(Settings settings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, plugins);
        }

        public TransportClientImpl(Settings settings, Settings defaultSettings, Collection<Class<? extends Plugin>> plugins) {
            super(settings, defaultSettings, plugins, null);
        }       
    }
    
    @SafeVarargs
    protected static Collection<Class<? extends Plugin>> asCollection(Class<? extends Plugin>... plugins) {
        return Arrays.asList(plugins);
    }
    
    
    protected TransportClient getInternalTransportClient(ClusterInfo info, Settings initTransportClientSettings) {
        
        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";
        
        Settings tcSettings = Settings.builder()
                .put("cluster.name", info.clustername)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, utFips()?false:allowOpenSSL)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, utFips()?false:allowOpenSSL)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore."+(utFips()?"BCFKS":"jks")))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.keystore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix+"kirk-keystore."+(utFips()?"BCFKS":"jks")))
                .put(initTransportClientSettings)
                .build();
        
        TransportClient tc = new TransportClientImpl(tcSettings, asCollection(Netty4Plugin.class, SearchGuardPlugin.class));
        tc.addTransportAddress(new TransportAddress(new InetSocketAddress(info.nodeHost, info.nodePort)));
        return tc;
    }
    
    protected String handleFips(String keyStore) {
        if(utFips()) {
            keyStore = keyStore.replace(".jks", ".BCFKS");
            keyStore = keyStore.replace(".p12", ".BCFKS");
        }
        
        return keyStore;
    }
    
    protected TransportClient getUserTransportClient(ClusterInfo info, String keyStore, Settings initTransportClientSettings) {
        
        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";
        
        Settings tcSettings = Settings.builder()
                .put("cluster.name", info.clustername)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, utFips()?false:allowOpenSSL)
                .put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, utFips()?false:allowOpenSSL)
                .put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore."+(utFips()?"BCFKS":"jks")))
                .put("searchguard.ssl.transport.enforce_hostname_verification", false)
                .put("searchguard.ssl.transport.keystore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix+handleFips(keyStore)))
                .put(initTransportClientSettings)
                .build();
        
        TransportClient tc = new TransportClientImpl(tcSettings, asCollection(Netty4Plugin.class, SearchGuardPlugin.class));
        tc.addTransportAddress(new TransportAddress(new InetSocketAddress(info.nodeHost, info.nodePort)));
        return tc;
    }
    
    protected void initialize(ClusterInfo info, Settings initTransportClientSettings, DynamicSgConfig sgconfig) {

        try (TransportClient tc = getInternalTransportClient(info, initTransportClientSettings)) {

            tc.addTransportAddress(new TransportAddress(new InetSocketAddress(info.nodeHost, info.nodePort)));
            Assert.assertEquals(info.numNodes,
                    tc.admin().cluster().nodesInfo(new NodesInfoRequest()).actionGet().getNodes().size());

            try {
                tc.admin().indices().create(new CreateIndexRequest("searchguard")).actionGet();
            } catch (Exception e) {
                //ignore
            }
            
            for(IndexRequest ir: sgconfig.getDynamicConfig(getResourceFolder())) {
                tc.index(ir).actionGet();
            }

            ConfigUpdateResponse cur = tc
                    .execute(ConfigUpdateAction.INSTANCE, new ConfigUpdateRequest(CType.lcStringValues().toArray(new String[0])))
                    .actionGet();
            Assert.assertFalse(cur.failures().toString(), cur.hasFailures());
            Assert.assertEquals(info.numNodes, cur.getNodes().size());
            
            SearchResponse sr = tc.search(new SearchRequest("searchguard")).actionGet();
            //Assert.assertEquals(5L, sr.getHits().getTotalHits());

            sr = tc.search(new SearchRequest("searchguard")).actionGet();
            //Assert.assertEquals(5L, sr.getHits().getTotalHits());
            
            String type=sgconfig.getType();

            Assert.assertTrue(tc.get(new GetRequest("searchguard", type, "config")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard",type,"internalusers")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard",type,"roles")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard",type,"rolesmapping")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard",type,"actiongroups")).actionGet().isExists());
            Assert.assertFalse(tc.get(new GetRequest("searchguard",type,"rolesmapping_xcvdnghtu165759i99465")).actionGet().isExists());
            Assert.assertTrue(tc.get(new GetRequest("searchguard",type,"config")).actionGet().isExists());
        }
    }
    
    protected Settings.Builder minimumSearchGuardSettingsBuilder(int node, boolean sslOnly) {

        final String prefix = getResourceFolder()==null?"":getResourceFolder()+"/";

        Settings.Builder builder = Settings.builder();
                if(!sslOnly) {
                //.put("searchguard.ssl.transport.enabled", true)
                 //.put("searchguard.no_default_init", true)
                //.put("searchguard.ssl.http.enable_openssl_if_available", false)
                //.put("searchguard.ssl.transport.enable_openssl_if_available", false)
                    builder.put(SSLConfigConstants.SEARCHGUARD_SSL_HTTP_ENABLE_OPENSSL_IF_AVAILABLE, utFips()?false:allowOpenSSL);
                    builder.put(SSLConfigConstants.SEARCHGUARD_SSL_TRANSPORT_ENABLE_OPENSSL_IF_AVAILABLE, utFips()?false:allowOpenSSL);
                    builder.put("searchguard.ssl.transport.keystore_alias", "node-0");
                    builder.put("searchguard.ssl.transport.keystore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix+"node-0-keystore."+(utFips()?"BCFKS":"jks")));
                    builder.put("searchguard.ssl.transport.truststore_filepath",
                        FileHelper.getAbsoluteFilePathFromClassPath(prefix+"truststore."+(utFips()?"BCFKS":"jks")));
                    builder.put("searchguard.ssl.transport.enforce_hostname_verification", false);                
                    builder.putList("searchguard.authcz.admin_dn", "CN=kirk,OU=client,O=client,l=tEst, C=De");
                    builder.put(ConfigConstants.SEARCHGUARD_BACKGROUND_INIT_IF_SGINDEX_NOT_EXIST, false);
                    
                    builder.putList("searchguard.nodes_dn", "CN=node-*.example.com*");
                    builder.put("searchguard.cert.oid", "0.0.0.1.1.xxx.xxx");
                //.put(other==null?Settings.EMPTY:other);
                }
                
        return builder;
    }
    
    protected NodeSettingsSupplier minimumSearchGuardSettings(Settings other) {
        return new NodeSettingsSupplier() {
            @Override
            public Settings get(int i) {
                return minimumSearchGuardSettingsBuilder(i, false).put(other).build();
            }
        };
    }
    
    protected NodeSettingsSupplier minimumSearchGuardSettingsSslOnly(Settings other) {
        return new NodeSettingsSupplier() {
            @Override
            public Settings get(int i) {
                return minimumSearchGuardSettingsBuilder(i, true).put(other).build();
            }
        };
    }
    
    protected void initialize(ClusterInfo info) {
        initialize(info, Settings.EMPTY, new DynamicSgConfig());
    }
    
    protected void initialize(ClusterInfo info, DynamicSgConfig dynamicSgConfig) {
        initialize(info, Settings.EMPTY, dynamicSgConfig);
    }
    
    protected final void assertContains(HttpResponse res, String pattern) {
        Assert.assertTrue(WildcardMatcher.match(pattern, res.getBody()));
    }
    
    protected final void assertNotContains(HttpResponse res, String pattern) {
        Assert.assertFalse(WildcardMatcher.match(pattern, res.getBody()));
    }
    
    protected String getResourceFolder() {
        return null;
    }
    
    protected String getType() {
        return "_doc";
    }
    
    protected static boolean utFips() {
        return CryptoManagerFactory.isFipsEnabled();
    }
}
