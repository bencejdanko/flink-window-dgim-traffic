package com.bencejdanko.flink;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * DGIMAccumulator maintains the state of the DGIM algorithm within a window.
 * It keeps track of buckets of 1-bits and provides methods to add new bits,
 * merge buckets according to DGIM rules, expire old buckets, and estimate counts.
 */
public class DGIMAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private long windowSizeSeconds;
    private List<Bucket> buckets;
    private long latestTimestamp;
    
    /**
     * Default constructor for serialization
     */
    public DGIMAccumulator() {
        this.windowSizeSeconds = 60; // Default window size
        this.buckets = new ArrayList<>();
        this.latestTimestamp = 0;
    }
    
    /**
     * Creates a new DGIMAccumulator with the specified window size
     * 
     * @param windowSizeSeconds The duration of the DGIM window in seconds
     */
    public DGIMAccumulator(long windowSizeSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
        this.buckets = new ArrayList<>();
        this.latestTimestamp = 0;
    }
    
    /**
     * Adds a new 1-bit at the specified timestamp
     * 
     * @param timestampSeconds The timestamp in seconds since epoch
     */
    public void add(long timestampSeconds) {
        // Update latest timestamp seen
        this.latestTimestamp = Math.max(this.latestTimestamp, timestampSeconds);
        
        // Add a new bucket of size 1 at the beginning of the list (newest first)
        this.buckets.add(0, new Bucket(timestampSeconds, 1));
        
        // Apply DGIM rules: merge buckets and expire old ones
        mergeBuckets();
        expireBuckets(timestampSeconds);
    }
    
    /**
     * Calculates the DGIM count estimate
     * 
     * @return The estimated count of 1-bits in the window
     */
    public long estimate() {
        // First, ensure we're only considering relevant buckets
        expireBuckets(this.latestTimestamp);
        
        // Handle edge cases
        if (buckets.isEmpty()) {
            return 0;
        }
        
        if (buckets.size() == 1) {
            return buckets.get(0).getSize();
        }
        
        // Compute the sum according to DGIM estimation rule:
        // Sum all bucket sizes, but halve the size of the oldest bucket
        long sum = 0;
        for (int i = 0; i < buckets.size() - 1; i++) {
            sum += buckets.get(i).getSize();
        }
        
        // Add half the size of the oldest bucket (rounding up when odd)
        int oldestSize = buckets.get(buckets.size() - 1).getSize();
        sum += (oldestSize + 1) / 2;
        
        return sum;
    }
    
    /**
     * Merges this accumulator with another one
     * 
     * @param other The other accumulator to merge with
     * @return A new merged accumulator
     */
    public DGIMAccumulator merge(DGIMAccumulator other) {
        // Create a new accumulator
        DGIMAccumulator merged = new DGIMAccumulator(this.windowSizeSeconds);
        
        // Set latest timestamp to the maximum of both accumulators
        merged.latestTimestamp = Math.max(this.latestTimestamp, other.latestTimestamp);
        
        // Combine all buckets from both accumulators
        merged.buckets.addAll(this.buckets);
        merged.buckets.addAll(other.buckets);
        
        // Sort buckets by timestamp (newest first)
        merged.buckets.sort(Comparator.comparing(Bucket::getTimestamp).reversed());
        
        // Apply DGIM rules to the merged accumulator
        merged.mergeBuckets();
        merged.expireBuckets(merged.latestTimestamp);
        
        return merged;
    }
    
    /**
     * Merges buckets according to the DGIM rule: no more than 2 buckets of the same size
     */
    private void mergeBuckets() {
        // We need at least 3 buckets to check for merges
        if (buckets.size() < 3) {
            return;
        }
        
        int i = 0;
        while (i < buckets.size() - 2) {
            // Look for three consecutive buckets of the same size
            if (buckets.get(i).getSize() == buckets.get(i + 1).getSize() && 
                buckets.get(i + 1).getSize() == buckets.get(i + 2).getSize()) {
                
                // Merge the middle two buckets:
                // - Use timestamp of the middle bucket
                // - Double the size
                // - Remove the third bucket
                Bucket middle = buckets.get(i + 1);
                middle.setSize(middle.getSize() * 2);
                buckets.remove(i + 2);
                
                // Start over since the sizes have changed
                i = 0;
            } else {
                i++;
            }
        }
    }
    
    /**
     * Removes buckets that are older than the window size
     * 
     * @param currentTimestampSeconds The current timestamp in seconds
     */
    private void expireBuckets(long currentTimestampSeconds) {
        Iterator<Bucket> it = buckets.iterator();
        while (it.hasNext()) {
            Bucket bucket = it.next();
            if (currentTimestampSeconds - bucket.getTimestamp() >= this.windowSizeSeconds) {
                it.remove();
            }
        }
    }
    
    // Getters and setters for serialization
    
    public long getWindowSizeSeconds() {
        return windowSizeSeconds;
    }
    
    public void setWindowSizeSeconds(long windowSizeSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
    }
    
    public List<Bucket> getBuckets() {
        return buckets;
    }
    
    public void setBuckets(List<Bucket> buckets) {
        this.buckets = buckets;
    }
    
    public long getLatestTimestamp() {
        return latestTimestamp;
    }
    
    public void setLatestTimestamp(long latestTimestamp) {
        this.latestTimestamp = latestTimestamp;
    }
}