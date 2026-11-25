package queueless.concurrency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import queueless.model.ServiceType;
import queueless.model.Token;

/**
 * Bounded priority queue implemented with a monitor: guarded blocks with wait()/notifyAll().
 * Producers (token generation) wait while the queue is full; consumers (counters) wait while
 * no token of their service type is available. Always called inside synchronized methods and
 * always re-checks the condition in a while-loop (guards against spurious wake-ups).
 */
public class TokenQueue {

    private final PriorityQueue<Token> queue = new PriorityQueue<>();
    private final int capacity;
    private boolean closed;

    public TokenQueue(int capacity) { this.capacity = capacity; }

    /** Producer side: blocks while the queue is full. */
    public synchronized void put(Token token) throws InterruptedException {
        while (queue.size() >= capacity && !closed) {
            wait();
        }
        if (closed) throw new IllegalStateException("Queue is closed");
        queue.add(token);
        notifyAll();                       // wake consumers waiting for a token
    }

    /** Non-blocking add used by the interactive menu. Returns false if full or closed. */
    public synchronized boolean offer(Token token) {
        if (closed || queue.size() >= capacity) return false;
        queue.add(token);
        notifyAll();
        return true;
    }

    /** Consumer side: blocks until a token of this service type exists; null when closed and none left. */
    public synchronized Token takeFor(ServiceType type) throws InterruptedException {
        while (!hasTokenFor(type) && !closed) {
            wait();
        }
        return removeBest(type);
    }

    /** Non-blocking version: returns null when nothing is waiting for this service. */
    public synchronized Token pollFor(ServiceType type) { return removeBest(type); }

    public synchronized boolean remove(Token token) {
        boolean removed = queue.remove(token);
        if (removed) notifyAll();          // a slot is free for waiting producers
        return removed;
    }

    public synchronized List<Token> snapshot() {
        List<Token> copy = new ArrayList<>(queue);
        Collections.sort(copy);
        return copy;
    }

    public synchronized int size() { return queue.size(); }

    public synchronized int capacity() { return capacity; }

    public synchronized void close() {
        closed = true;
        notifyAll();                       // release every waiting thread
    }

    public synchronized void reopen() { closed = false; }

    private boolean hasTokenFor(ServiceType type) {
        return queue.stream().anyMatch(t -> t.getServiceType() == type);
    }

    private Token removeBest(ServiceType type) {
        Token best = queue.stream()
                .filter(t -> t.getServiceType() == type)
                .min(Comparator.naturalOrder())
                .orElse(null);
        if (best != null) {
            queue.remove(best);
            notifyAll();                   // a slot is free for waiting producers
        }
        return best;
    }
}
