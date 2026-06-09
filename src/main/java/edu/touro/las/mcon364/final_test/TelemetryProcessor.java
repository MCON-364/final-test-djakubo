package edu.touro.las.mcon364.final_test;

import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TelemetryProcessor – concurrent sensor-data pipeline
 *
 * Scenario: a fleet of devices continuously emits telemetry readings.
 * Each reading is represented as a {@link TelemetryEvent} carrying a device id,
 * a numeric metric value, and a nanosecond timestamp. Readings arrive faster than
 * they can be processed synchronously, so a multi-worker, queue-based pipeline
 * is required.
 *
 * Requirements:
 * - submit(event) enqueues an event so a worker thread can process it.
 *   It must throw {@link IllegalArgumentException} if event is null.
 *   Events submitted before start() is called must be silently discarded.
 * - start(workerCount) spins up {@code workerCount} worker threads that continuously
 *   drain the queue and process events. It must throw {@link IllegalArgumentException}
 *   if workerCount ≤ 0. Calling start() a second time must be a no-op(should make no difference).
 * - stop() signals all workers to finish, waits for them to terminate, then processes
 *   any events still left in the queue before returning.
 * - getTotalProcessed() returns the running total of events fully processed.
 * - getStats() returns a {@link DoubleSummaryStatistics} snapshot of all processed
 *   metric values. Each call must return a fresh, independent object.
 *
 * Thread-safety requirements:
 * - submit() and the read methods (getTotalProcessed, getStats) may be called
 *   concurrently from multiple threads without data loss or corruption.
 * - Use java.util.concurrent building blocks. Do not use raw synchronized blocks.
 */
public class TelemetryProcessor {

    // ── declare whatever fields you need ─────────────────────────────────────
    private LinkedBlockingQueue<TelemetryEvent> events = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<TelemetryEvent> processedEvents = new LinkedBlockingQueue<>();
    private boolean running = false;
    private ExecutorService pool;
    private AtomicInteger totalProcessed = new AtomicInteger();
    // ── public API ────────────────────────────────────────────────────────────

    /**
     * Add an event to the processing queue.
     *
     * Events submitted before {@link #start(int)} is called must be silently discarded.
     *
     * @param event the telemetry event to enqueue; must not be null
     * @throws IllegalArgumentException if event is null
     */
    public void submit(TelemetryEvent event) {
        //TODO - implement this method
        if(event==null){
            throw new IllegalArgumentException("Event cannot be null");
        }
        if(running){
            events.add(event);
        }
    }

    /**
     * Start processing events.
     * @param workerCount number of worker threads to create; must be ≥ 1
     * @throws IllegalArgumentException if workerCount ≤ 0
     */
    public void start(int workerCount) {
        //TODO - implement this method
        if(workerCount<=0){throw new IllegalArgumentException("Worker count must be positive");}
        pool = Executors.newFixedThreadPool(workerCount);
        running = true;
        for(int i = 0; i< workerCount; i++){
            pool.submit(this::workerLoop);
        }
    }

    private void workerLoop(){
        // TODO: implement
        try{
            while(true){
                processedEvents.offer(events.take());
                totalProcessed.getAndIncrement();
            }
        } catch(InterruptedException e){
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Stop processing events.
     * @throws InterruptedException if the calling thread is interrupted while waiting
     */
    public void stop() throws InterruptedException {
        //TODO - implement this method
        if(pool==null){
            return;
        }
        if(!pool.awaitTermination(1, TimeUnit.SECONDS)){
            running = false;
            pool.shutdownNow();
        }
    }

    /**
     * Return the total number of events that have been fully processed.
     */
    public int getTotalProcessed() {
        //TODO - implement this method
        return totalProcessed.get();
    }

    /**
     * Return a point-in-time snapshot of summary statistics for all processed
     * metric values (count, sum, min, max, average).
     *
     * Each call must return a <em>new</em>, independent {@link DoubleSummaryStatistics}
     * object so that callers cannot corrupt the internal state.
     *
     */
    public DoubleSummaryStatistics getStats() {
        //TODO - implement this method
        if(totalProcessed.get()==0){
            return new DoubleSummaryStatistics();
        }
        DoubleSummaryStatistics stats = processedEvents.stream().mapToDouble(TelemetryEvent::metric).summaryStatistics();
        return stats;
    }
}
