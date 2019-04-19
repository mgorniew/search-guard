package com.floragunn.searchguard.sgconf.impl.v7;

import java.util.Collections;
import java.util.List;

import com.floragunn.searchguard.sgconf.Hideable;
import com.floragunn.searchguard.sgconf.impl.v6.ActionGroupsV6;

public class ActionGroupsV7 implements Hideable {

    
    
    private boolean reserved;
    private boolean hidden;
    private List<String> permissions = Collections.emptyList();
    private String type;
    private String description;
    
    public ActionGroupsV7() {
        super();
    }
    public ActionGroupsV7(String agName, ActionGroupsV6 ag6) {
        reserved = ag6.isReserved();
        hidden = ag6.isHidden();
        permissions = ag6.getPermissions();
        type = agName.toLowerCase().contains("cluster")?"cluster":"index";
        description = "Migrated from v6";
    }

    public ActionGroupsV7(String key, List<String> perms) {
        permissions = perms;
        type = "unknown";
        description = "Migrated from v6 (legacy)";
    }
    public String getType() {
        return type;
    }
    public void setType(String type) {
        this.type = type;
    }
    public String getDescription() {
        return description;
    }
    public void setDescription(String description) {
        this.description = description;
    }
    
    
    public boolean isReserved() {
        return reserved;
    }
    public void setReserved(boolean reserved) {
        this.reserved = reserved;
    }
    public boolean isHidden() {
        return hidden;
    }
    public void setHidden(boolean hidden) {
        this.hidden = hidden;
    }
    public List<String> getPermissions() {
        return permissions;
    }
    public void setPermissions(List<String> permissions) {
        this.permissions = permissions;
    }
    @Override
    public String toString() {
        return "ActionGroups [reserved=" + reserved + ", hidden=" + hidden + ", permissions=" + permissions + "]";
    }
    
    
}