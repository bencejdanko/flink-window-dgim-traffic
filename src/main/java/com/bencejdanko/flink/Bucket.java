package com.bencejdanko.flink;

import java.io.Serializable;

/**
 * Represents a single bucket in the DGIM algorithm.
 * Each bucket stores a timestamp and size.
 */
public class Bucket implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private long timestamp; // seconds since epoch
    private int size;
    
    /**
     * Default constructor for serialization
     */
    public Bucket() {
        this.timestamp = 0;
        this.size = 0;
    }
    
    /**
     * Create a new bucket with the specified timestamp and size
     *
     * @param timestamp the timestamp in seconds since epoch
     * @param size the size of the bucket
     */
    public Bucket(long timestamp, int size) {
        this.timestamp = timestamp;
        this.size = size;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
    
    public int getSize() {
        return size;
    }
    
    public void setSize(int size) {
        this.size = size;
    }
    
    @Override
    public String toString() {
        return "Bucket{timestamp=" + timestamp + ", size=" + size + '}';
    }
}