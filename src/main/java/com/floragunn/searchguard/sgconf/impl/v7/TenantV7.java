package com.floragunn.searchguard.sgconf.impl.v7;

import com.floragunn.searchguard.sgconf.Hideable;

public class TenantV7 implements Hideable {

    private boolean reserved;
    private boolean hidden;
    private String description;
    
    public boolean isHidden() {
        return hidden;
    }
    public void setHidden(boolean hidden) {
        this.hidden = hidden;
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
    @Override
    public String toString() {
        return "TenantV7 [reserved=" + reserved + ", hidden=" + hidden + ", description=" + description + "]";
    }
    
    
}