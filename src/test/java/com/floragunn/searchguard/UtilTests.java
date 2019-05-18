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

package com.floragunn.searchguard;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Provider;
import java.security.Signature;
import java.security.spec.InvalidKeySpecException;
import java.util.Map;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.common.settings.Settings;
import org.junit.Assert;
import org.junit.Test;

import com.floragunn.searchguard.crypto.CryptoManagerFactory;
import com.floragunn.searchguard.support.ConfigConstants;
import com.floragunn.searchguard.support.SgUtils;
import com.floragunn.searchguard.support.WildcardMatcher;
import com.floragunn.searchguard.test.AbstractSGUnitTest;

public class UtilTests extends AbstractSGUnitTest {
    
    @Test
    public void testWildcards() {
        Assert.assertTrue(!WildcardMatcher.match("a*?", "a"));
        Assert.assertTrue(WildcardMatcher.match("a*?", "aa"));
        Assert.assertTrue(WildcardMatcher.match("a*?", "ab"));
        //Assert.assertTrue(WildcardMatcher.match("a*?", "abb"));
        Assert.assertTrue(WildcardMatcher.match("*my*index", "myindex"));
        Assert.assertTrue(!WildcardMatcher.match("*my*index", "myindex1"));
        Assert.assertTrue(WildcardMatcher.match("*my*index?", "myindex1"));
        Assert.assertTrue(WildcardMatcher.match("*my*index", "this_is_my_great_index"));
        Assert.assertTrue(!WildcardMatcher.match("*my*index", "MYindex"));
        Assert.assertTrue(!WildcardMatcher.match("?kibana", "kibana"));
        Assert.assertTrue(WildcardMatcher.match("?kibana", ".kibana"));
        Assert.assertTrue(!WildcardMatcher.match("?kibana", "kibana."));
        Assert.assertTrue(WildcardMatcher.match("?kibana?", "?kibana."));
        Assert.assertTrue(WildcardMatcher.match("/(\\d{3}-?\\d{2}-?\\d{4})/", "123-45-6789"));
        Assert.assertTrue(!WildcardMatcher.match("(\\d{3}-?\\d{2}-?\\d{4})", "123-45-6789"));
        Assert.assertTrue(WildcardMatcher.match("/\\S*/", "abc"));
        Assert.assertTrue(WildcardMatcher.match("abc", "abc"));
        Assert.assertTrue(!WildcardMatcher.match("ABC", "abc"));
        Assert.assertTrue(!WildcardMatcher.containsWildcard("abc"));
        Assert.assertTrue(!WildcardMatcher.containsWildcard("abc$"));
        Assert.assertTrue(WildcardMatcher.containsWildcard("abc*"));
        Assert.assertTrue(WildcardMatcher.containsWildcard("a?bc"));
        Assert.assertTrue(WildcardMatcher.containsWildcard("/(\\d{3}-\\d{2}-?\\d{4})/"));
    }

    @Test
    public void testMapFromArray() {
        Map<Object, Object> map = SgUtils.mapFromArray((Object)null);
        assertTrue(map == null);
        
        map = SgUtils.mapFromArray("key");
        assertTrue(map == null);

        map = SgUtils.mapFromArray("key", "value", "otherkey");
        assertTrue(map == null);
        
        map = SgUtils.mapFromArray("key", "value");
        assertNotNull(map);        
        assertEquals(1, map.size());
        assertEquals("value", map.get("key"));

        map = SgUtils.mapFromArray("key", "value", "key", "value");
        assertNotNull(map);        
        assertEquals(1, map.size());
        assertEquals("value", map.get("key"));

        map = SgUtils.mapFromArray("key1", "value1", "key2", "value2");
        assertNotNull(map);        
        assertEquals(2, map.size());
        assertEquals("value1", map.get("key1"));
        assertEquals("value2", map.get("key2"));

    }
    
    @Test
    public void testEnvReplace() throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException {
        Settings settings = Settings.EMPTY;
        Assert.assertEquals("abv${env.MYENV}xyz", SgUtils.replaceEnvVars("abv${env.MYENV}xyz",settings));
        Assert.assertEquals("abv${envbc.MYENV}xyz", SgUtils.replaceEnvVars("abv${envbc.MYENV}xyz",settings));
        Assert.assertEquals("abvtTtxyz", SgUtils.replaceEnvVars("abv${env.MYENV:-tTt}xyz",settings));
        Assert.assertTrue(CryptoManagerFactory.getInstance().checkPasswordHash(SgUtils.replaceEnvVars("${envbc.MYENV:-tTt}",settings), "tTt".toCharArray()));
        Assert.assertEquals("abvtTtxyzxxx", SgUtils.replaceEnvVars("abv${env.MYENV:-tTt}xyz${env.MYENV:-xxx}",settings));
        String enc = SgUtils.replaceEnvVars("abv${env.MYENV:-tTt}xyz${envbc.MYENV:-xxx}",settings);
        Assert.assertTrue(enc, enc.startsWith("abvtTtxyz") && enc.contains("#"));
        Assert.assertEquals("abv${env.MYENV:tTt}xyz", SgUtils.replaceEnvVars("abv${env.MYENV:tTt}xyz",settings));
        Assert.assertEquals("abv${env.MYENV-tTt}xyz", SgUtils.replaceEnvVars("abv${env.MYENV-tTt}xyz",settings));
        //Assert.assertEquals("abvabcdefgxyz", SgUtils.replaceEnvVars("abv${envbase64.B64TEST}xyz",settings));

        Map<String, String> env = System.getenv();
        Assert.assertTrue(env.size() > 0);
        
        boolean checked = false;

        for(String k: env.keySet()) {
            String val=System.getenv().get(k);
            if(val == null || val.isEmpty()) {
                continue;
            }
            Assert.assertEquals("abv"+val+"xyz", SgUtils.replaceEnvVars("abv${env."+k+"}xyz",settings));
            Assert.assertEquals("abv${"+k+"}xyz", SgUtils.replaceEnvVars("abv${"+k+"}xyz",settings));
            Assert.assertEquals("abv"+val+"xyz", SgUtils.replaceEnvVars("abv${env."+k+":-k182765ggh}xyz",settings));
            Assert.assertEquals("abv"+val+"xyzabv"+val+"xyz", SgUtils.replaceEnvVars("abv${env."+k+"}xyzabv${env."+k+"}xyz",settings));
            Assert.assertEquals("abv"+val+"xyz", SgUtils.replaceEnvVars("abv${env."+k+":-k182765ggh}xyz",settings));
            Assert.assertTrue(CryptoManagerFactory.getInstance().checkPasswordHash(SgUtils.replaceEnvVars("${envbc."+k+"}",settings), val.toCharArray()));
            checked = true;
        }
        
        Assert.assertTrue(checked);
    }
    
    @Test
    public void testNoEnvReplace() {
        Settings settings = Settings.builder().put(ConfigConstants.SEARCHGUARD_DISABLE_ENVVAR_REPLACEMENT, true).build();
        Assert.assertEquals("abv${env.MYENV}xyz", SgUtils.replaceEnvVars("abv${env.MYENV}xyz",settings));
        Assert.assertEquals("abv${envbc.MYENV}xyz", SgUtils.replaceEnvVars("abv${envbc.MYENV}xyz",settings));
        Assert.assertEquals("abv${env.MYENV:-tTt}xyz", SgUtils.replaceEnvVars("abv${env.MYENV:-tTt}xyz",settings));
        Assert.assertEquals("abv${env.MYENV:-tTt}xyz${env.MYENV:-xxx}", SgUtils.replaceEnvVars("abv${env.MYENV:-tTt}xyz${env.MYENV:-xxx}",settings));
        Assert.assertFalse(SgUtils.replaceEnvVars("abv${env.MYENV:-tTt}xyz${envbc.MYENV:-xxx}",settings).startsWith("abvtTtxyz$2y$"));
        Assert.assertEquals("abv${env.MYENV:tTt}xyz", SgUtils.replaceEnvVars("abv${env.MYENV:tTt}xyz",settings));
        Assert.assertEquals("abv${env.MYENV-tTt}xyz", SgUtils.replaceEnvVars("abv${env.MYENV-tTt}xyz",settings));
        Map<String, String> env = System.getenv();
        Assert.assertTrue(env.size() > 0);
        
        for(String k: env.keySet()) {
            Assert.assertEquals("abv${env."+k+"}xyz", SgUtils.replaceEnvVars("abv${env."+k+"}xyz",settings));
            Assert.assertEquals("abv${"+k+"}xyz", SgUtils.replaceEnvVars("abv${"+k+"}xyz",settings));
            Assert.assertEquals("abv${env."+k+":-k182765ggh}xyz", SgUtils.replaceEnvVars("abv${env."+k+":-k182765ggh}xyz",settings));
            Assert.assertEquals("abv${env."+k+"}xyzabv${env."+k+"}xyz", SgUtils.replaceEnvVars("abv${env."+k+"}xyzabv${env."+k+"}xyz",settings));
            Assert.assertEquals("abv${env."+k+":-k182765ggh}xyz", SgUtils.replaceEnvVars("abv${env."+k+":-k182765ggh}xyz",settings));
        }
    }
    
    @Test
    public void testSubstringBetween() throws Exception {
        String test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "", "", true);
        Assert.assertEquals("", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "{", "}", true);
        Assert.assertEquals("{xxx}", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "bb{", "xx}", true);
        Assert.assertEquals("bb{xxx}", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "bb{", "xx}", false);
        Assert.assertEquals("x", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "bb{x", "xx}", true);
        Assert.assertEquals("bb{xxx}", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "bb{x", "xx}", false);
        Assert.assertEquals("", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "bb{x", "ddxx}", true);
        Assert.assertEquals("aaa\bbb{xxx}\n\n", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "{", "}", false);
        Assert.assertEquals("xxx", test);
        test = SgUtils.substringBetween("aaa\bbb{xxx}\n\n", "{x", "\n", false);
        Assert.assertEquals("xx}", test);
    }
    
    @Test
    public void testSignatureAlgoAvailable() throws Exception {
        Provider p = Signature.getInstance("SHA512withRSA").getProvider();
        System.out.println("SHA512withRSA supported by "+p.getName());
        p = Signature.getInstance("SHA512withECDSA").getProvider();
        System.out.println("SHA512withECDSA supported by "+p.getName());
        p = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512").getProvider(); 
        System.out.println("PBKDF2WithHmacSHA512 supported by "+p.getName());
        PBEKeySpec pbeSpec = new PBEKeySpec("123456789123456789123456789".toCharArray(), "123456789123456789123456789".getBytes(), 10000, 256); //32 byte key len
        byte[] hash = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512").generateSecret(pbeSpec).getEncoded();
        System.out.println(hash.length);
        System.out.println(Base64.encodeBase64String(hash).length());
    }
}
