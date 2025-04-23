package com.bencejdanko.flink;

import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DGIMAggregateFunction implements Flink's TableAggregateFunction interface to integrate
 * the DGIM algorithm into Flink SQL. It counts 1-bits in a stream and provides
 * an estimate of their count over a sliding window.
 */
public class DGIMAggregateFunction extends TableAggregateFunction<Long, DGIMAccumulator> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(DGIMAggregateFunction.class);
    
    private long windowSizeSeconds = 60; // Default value for no-arg constructor

    /**
     * Default constructor for Flink reflection
     */
    public DGIMAggregateFunction() {
        // Default window size
    }

    /**
     * Creates a new DGIMAggregateFunction with the specified window size
     * 
     * @param windowSizeSeconds The duration of the DGIM window in seconds
     */
    public DGIMAggregateFunction(long windowSizeSeconds) {
        this.windowSizeSeconds = windowSizeSeconds;
    }
    
    /**
     * Creates a new accumulator for the aggregation
     */
    @Override
    public DGIMAccumulator createAccumulator() {
        return new DGIMAccumulator(this.windowSizeSeconds);
    }
    
    /**
     * Processes a row by adding its value to the accumulator if it's a 1-bit
     * 
     * @param acc The accumulator
     * @param value The value string to parse as an integer
     * @param timestampSeconds The timestamp in seconds since epoch
     */
    public void accumulate(DGIMAccumulator acc, String value, Long timestampSeconds) {
        // Skip processing for null values
        if (value == null || timestampSeconds == null) {
            return;
        }
        
        try {
            // Try to parse the value as an integer
            int bit = Integer.parseInt(value);
            
            // Only process 1-bits
            if (bit == 1) {
                acc.add(timestampSeconds);
                LOG.debug("Added 1-bit at timestamp {}", timestampSeconds);
            }
        } catch (NumberFormatException e) {
            // Log the error but don't fail the job
            LOG.warn("Failed to parse value '{}' as integer: {}", value, e.getMessage());
        }
    }

    /**
     * Emits the result of the aggregation
     * 
     * @param acc The accumulator containing the aggregation state
     * @param out The collector to emit the result
     */
    public void emitValue(DGIMAccumulator acc, Collector<Long> out) {
        long estimate = acc.estimate();
        LOG.debug("DGIM estimate: {}", estimate);
        out.collect(estimate);
    }

    /**
     * Merges a group of accumulators into one accumulator
     * 
     * @param acc The accumulator to merge into
     * @param it The iterable of accumulators to merge from
     */
    public void merge(DGIMAccumulator acc, Iterable<DGIMAccumulator> it) {
        for (DGIMAccumulator other : it) {
            DGIMAccumulator merged = acc.merge(other);
            acc.setBuckets(merged.getBuckets());
            acc.setLatestTimestamp(merged.getLatestTimestamp());
            acc.setWindowSizeSeconds(merged.getWindowSizeSeconds());
        }
    }
}