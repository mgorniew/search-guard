package com.floragunn.searchguard.auth.blocking;

public interface ClientBlockRegistry<ClientIdType> {

    boolean isBlocked(ClientIdType clientId);
    void block(ClientIdType clientId);
    Class<ClientIdType> getClientIdType();
}
