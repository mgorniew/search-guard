package com.floragunn.searchguard.auth.limiting;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.floragunn.searchguard.util.ratetracking.HeapBasedRateTracker;

public class HeapBasedRateTrackerTest {
    
    @Test
    public void simpleTest() throws Exception {   
        HeapBasedRateTracker<String> tracker = new HeapBasedRateTracker<>(100, 5, 100_000);
        
        assertFalse(tracker.track("a"));
        assertFalse(tracker.track("a"));
        assertFalse(tracker.track("a"));
        assertFalse(tracker.track("a"));
        assertTrue(tracker.track("a"));

    }
    
    @Test
    public void expiryTest() throws Exception {   
        HeapBasedRateTracker<String> tracker = new HeapBasedRateTracker<>(100, 5, 100_000);
        
        assertFalse(tracker.track("a"));
        assertFalse(tracker.track("a"));
        assertFalse(tracker.track("a"));
        assertFalse(tracker.track("a"));
        assertTrue(tracker.track("a"));

        assertFalse(tracker.track("b"));
        assertFalse(tracker.track("b"));
        assertFalse(tracker.track("b"));
        assertFalse(tracker.track("b"));
        assertTrue(tracker.track("b"));
        
        assertFalse(tracker.track("c"));    
        
        Thread.sleep(50);
        
        assertFalse(tracker.track("c"));   
        assertFalse(tracker.track("c"));      
        assertFalse(tracker.track("c"));   
        
        Thread.sleep(55); 
        
        assertFalse(tracker.track("c"));        
        assertTrue(tracker.track("c"));        

        assertFalse(tracker.track("a"));     
        
        Thread.sleep(55);
        assertFalse(tracker.track("c"));        
        assertFalse(tracker.track("c"));        
        assertTrue(tracker.track("c"));        

        
    }
    
    @Test
    public void maxTwoTriesTest() throws Exception {   
        HeapBasedRateTracker<String> tracker = new HeapBasedRateTracker<>(100, 2, 100_000);
        
        assertFalse(tracker.track("a"));
        assertTrue(tracker.track("a"));
        
        assertFalse(tracker.track("b"));
        Thread.sleep(50);
        assertTrue(tracker.track("b"));

        Thread.sleep(55);
        assertTrue(tracker.track("b"));

        Thread.sleep(105);
        assertFalse(tracker.track("b"));
        assertTrue(tracker.track("b"));

    }
}
