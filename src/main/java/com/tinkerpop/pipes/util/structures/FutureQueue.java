package com.tinkerpop.pipes.util.structures;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;

/**
 * A FutureQueue is a blocking queue to store results of prefetch threads.
 * Created by whshev on 15/1/8.
 */
public class FutureQueue<T> {

    private ArrayBlockingQueue<Future<T>> futureQueue;
    private int maxsize;

    public FutureQueue(int queueSize) {
        this.futureQueue = new ArrayBlockingQueue<Future<T>>(queueSize);
        this.maxsize = queueSize;
    }

    public void addFuture(Future<T> future){
        try {
            this.futureQueue.put(future);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public int size() {
        return this.futureQueue.size();
    }

    //When half of the queue has a value, the prefetch thread will be blocked.
    public boolean needAdd() {
        return this.futureQueue.size() * 2 <= this.maxsize;
    }

    public boolean canAdd() {
        return this.futureQueue.size() < this.maxsize;
    }

    public boolean hasNextFuture() {
        return !(this.futureQueue.isEmpty());
    }

    public Future<T> getNextFuture() {
        return this.futureQueue.poll();
    }
}
