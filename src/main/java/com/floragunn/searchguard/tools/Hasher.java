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

package com.floragunn.searchguard.tools;

import java.io.Console;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import com.floragunn.searchguard.crypto.CryptoManagerFactory;

public class Hasher {

    public static void main(final String[] args) {
        final Options options = new Options();
        final HelpFormatter formatter = new HelpFormatter();
        options.addOption(Option.builder("p").argName("password").hasArg().desc("Cleartext password to hash").build());
        options.addOption(Option.builder("env").argName("name environment variable").hasArg().desc("name environment variable to read password from").build());
        options.addOption(Option.builder("type").argName("bcrypt, pbkdf2 or fips").hasArg().desc("Type of hash (bcrypt is default)").build());
        
        final CommandLineParser parser = new DefaultParser();
        try {
            final CommandLine line = parser.parse(options, args);
            String fipsMode = line.getOptionValue("type");
            
            if(line.hasOption("p")) {
                System.out.println(hash(line.getOptionValue("p").toCharArray(), fipsMode));
            } else if(line.hasOption("env")) {
                final String pwd = System.getenv(line.getOptionValue("env"));
                if(pwd == null || pwd.isEmpty()) {
                    throw new Exception("No environment variable '"+line.getOptionValue("env")+"' set");
                }
                System.out.println(hash(pwd.toCharArray(), fipsMode));
            } else {
                final Console console = System.console();
                if(console == null) {
                    throw new Exception("Cannot allocate a console");
                }
                final char[] passwd = console.readPassword("[%s]", "Password:");
                System.out.println(hash(passwd, fipsMode));
            }  
        } catch (final Exception exp) {
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("hasher.sh", options, true);
            System.exit(-1);
        }
    }

    private static String hash(final char[] clearTextPassword, String fipsMode) {
        
        if(clearTextPassword == null || clearTextPassword.length == 0) {
            throw new RuntimeException("Empty passwords are not allowed");
        }
        
        try {
            final String hash;
            
            if("pbkdf2".equalsIgnoreCase(fipsMode)) {
                CryptoManagerFactory.initialize(false);
                hash = CryptoManagerFactory.getInstance().generatePBKDF2PasswordHash(clearTextPassword, null);
            } else if("fips".equalsIgnoreCase(fipsMode)) {
                
                if(System.getSecurityManager() == null) {
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
                }
                
                CryptoManagerFactory.initialize(true);
                hash = CryptoManagerFactory.getInstance().generatePasswordHash(clearTextPassword);
            } else {
                CryptoManagerFactory.initialize(false);
                hash = CryptoManagerFactory.getInstance().generatePasswordHash(clearTextPassword);
            }
            
            return hash;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if(clearTextPassword != null) {
                Arrays.fill(clearTextPassword, '\0');
            }
        }
    }
}
