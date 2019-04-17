package com.floragunn.searchguard.util.ratetracking;

public class SingleTryRateTracker<ClientIdType> implements RateTracker<ClientIdType> {

    @Override
    public boolean track(ClientIdType clientId) {
        return true;
    }

    @Override
    public void reset(ClientIdType clientId) {
    }
}
