package com.floragunn.searchguard.util.ratetracking;

public interface RateTracker<ClientIdType> {

    boolean track(ClientIdType clientId);

    void reset(ClientIdType clientId);

    static <ClientIdType> RateTracker<ClientIdType> create(long timeWindowMs, int allowedTries, int maxEntries) {
        if (allowedTries == 1) {
            return new SingleTryRateTracker<ClientIdType>();
        } else if (allowedTries > 1) {
            return new HeapBasedRateTracker<ClientIdType>(timeWindowMs, allowedTries, maxEntries);
        } else {
            throw new IllegalArgumentException("allowedTries must be > 0: " + allowedTries);
        }
    }

}
