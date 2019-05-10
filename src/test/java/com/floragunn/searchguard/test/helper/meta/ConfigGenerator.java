package com.floragunn.searchguard.test.helper.meta;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import com.floragunn.searchguard.support.SearchGuardDeprecationHandler;

public class ConfigGenerator {

    protected static final Logger log = LogManager.getLogger("myclass");
    
    public static void main(String[] args) throws IOException {
        toSettings0();
    }
    
    private static boolean uploadFile(final Client tc, final String filepath, final String index, final String _id, final boolean legacy) {
        
        String type = "sg";
        String id = _id;
                
        if(legacy) {
            type = _id;
            id = "0";
        }
        
        System.out.println("Will update '"+type+"/" + id + "' with " + filepath+" "+(legacy?"(legacy mode)":""));
        
        try (Reader reader = new FileReader(filepath)) {

            final String res = tc
                    .index(new IndexRequest(index).type(type).id(id).setRefreshPolicy(RefreshPolicy.IMMEDIATE)
                            .source(_id, readXContent(reader, XContentType.YAML))).actionGet().getId();

            if (id.equals(res)) {
                System.out.println("   SUCC: Configuration for '" + _id + "' created or updated");
                return true;
            } else {
                System.out.println("   FAIL: Configuration for '" + _id
                        + "' failed for unknown reasons. Please consult the Elasticsearch logfile.");
            }
        } catch (Exception e) {
            System.out.println("   FAIL: Configuration for '" + _id + "' failed because of " + e.toString());
        }

        return false;
    }
    
    private static Tuple<Long, Settings> toSettings0() {
        

        try {
            
            
            BytesReference json = readXContent(new FileReader(new File("/Users/salyh/Downloads/sg_roles_mapping_ing.yml")), XContentType.YAML);
            
            Settings.builder().loadFromStream("dummy.json", new ByteArrayInputStream(json.toBytesRef().bytes), true);
            
            
            System.out.println("ok");
            
            final BytesReference ref = new BytesArray(FileUtils.readFileToByteArray(new File("/Users/salyh/Downloads/sg_roles_ing.yml")));
            System.out.println(ref.length());
            final String type = "roles";
            final long version = 2;
            if (ref == null || ref.length() == 0) {
                log.error("Empty or null byte reference for {}", type);
                return null;
            }
            
            XContentParser parser = null;
            
            parser = XContentHelper.createParser(NamedXContentRegistry.EMPTY, SearchGuardDeprecationHandler.INSTANCE, ref, XContentType.JSON);
            parser.nextToken();
            parser.nextToken();
         
            if(!type.equals((parser.currentName()))) {
                log.error("Cannot parse config for type {} because {}!={}", type, type, parser.currentName());
                return null;
            }
            
            parser.nextToken();
            
            return new Tuple<Long, Settings>(version, Settings.builder().loadFromStream("dummy.json", new ByteArrayInputStream(parser.binaryValue()), true).build());
        } catch (final IOException e) {
            log.error(e.toString(),e);
            throw ExceptionsHelper.convertToElastic(e);
        } finally {
            
        }
    }
    
    private static BytesReference readXContent(final Reader reader, final XContentType xContentType) throws IOException {
        BytesReference retVal;
        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(xContentType).createParser(NamedXContentRegistry.EMPTY, SearchGuardDeprecationHandler.INSTANCE, reader);
            parser.nextToken();
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.copyCurrentStructure(parser);
            retVal = BytesReference.bytes(builder);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        
        //validate
        Settings.builder().loadFromStream("dummy.json", new ByteArrayInputStream(BytesReference.toBytes(retVal)), true).build();
        return retVal;
    }
    
    
    public static void main0(String[] args) throws IOException {
        
        StringBuilder roles = new StringBuilder();
        StringBuilder rolesMapping = new StringBuilder();
        StringBuilder users = new StringBuilder();
        
        for(int i=0; i<4000; i++) {
            roles.append(createRole("sgg_perfrole_"+i));
            rolesMapping.append(createRoleMapping("sgg_perfrole_"+i));
            users.append(createUser("uperfg_"+i));
        }

        FileUtils.write(new File("perf_sg_roles.yml"), roles.toString(), "UTF-8");
        FileUtils.write(new File("perf_sg_roles_mapping.yml"), rolesMapping.toString(), "UTF-8");
        FileUtils.write(new File("perf_sg_internal_users.yml"), users.toString(), "UTF-8");
    }
    
    private static String createUser(String name) {
        return name+":\n" + 
                "  hash: $2a$12$n5nubfWATfQjSYHiWtUyeOxMIxFInUHOAx8VMmGmxFNPGpaBmeB.m\n" + 
                "  roles:\n"+
                "    - perf_1\n" + 
                "    - perf_2\n" + 
                "    - perf_3\n" + 
                "    - perf_4\n" + 
                "    - perf_5\n" + 
                "    - perf_6\n" + 
                "  #password is: nagilum\n\n";
    }
    
    private static String createRoleMapping(String role) {
        return role+":\n" + 
                "  backendroles:\n" + 
                "    - perf_1\n" + 
                "    - perf_2\n" + 
                "    - perf_3\n" + 
                "    - perf_4\n" + 
                "    - perf_5\n" + 
                "    - perf_6\n" + 
                "    - perf_7\n" + 
                "    - perf_8\n" + 
                "    - perf_9\n" + 
                "    - perf_10\n" + 
                "  users:\n" + 
                "    - uperf_1\n" + 
                "    - uperf_2\n" + 
                "    - uperf_3\n" + 
                "    - uperf_4\n" + 
                "    - uperf_5\n" + 
                "    - uperf_6\n\n";
    }
    
    private static String createRole(String name) {
        return name+":\n" + 
                "  readonly: true\n" + 
                "  cluster:\n" + 
                "      - CLUSTER_MONITOR\n" + 
                "      - CLUSTER_COMPOSITE_OPS\n" + 
                "      - cluster:admin/xpack/monitoring*\n" + 
                "      - indices:admin/template*\n" + 
                "      - indices:data/read/scroll*\n" + 
                "  indices:\n" + 
                "    '?kibana':\n" + 
                "      '*':\n" + 
                "        - INDICES_ALL\n" + 
                "    '?kibana-6':\n" + 
                "      '*':\n" + 
                "        - INDICES_ALL\n" + 
                "    '?kibana_*':\n" + 
                "      '*':\n" + 
                "        - INDICES_ALL\n" + 
                "    '?reporting*':\n" + 
                "      '*':\n" + 
                "        - INDICES_ALL\n" + 
                "    '?monitoring*':\n" + 
                "      '*':\n" + 
                "        - INDICES_ALL\n" + 
                "    '?tasks':\n" + 
                "      '*':\n" + 
                "        - INDICES_ALL\n" + 
                "    '*':\n" + 
                "      '*':\n" + 
                "        - \"indices:admin/aliases*\"\n\n";
    }

}
