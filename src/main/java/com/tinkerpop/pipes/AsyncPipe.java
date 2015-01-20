package com.tinkerpop.pipes;

import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * An AsyncPipe includes an asynchronous thread to prefetch incoming S from the previous Pipe.
 * All the AsyncPipes in a Pipeline share one ExecutorService to manage their prefetch threads.
 * Created by whshev on 15/1/12.
 */
public abstract class AsyncPipe<S, E> extends AbstractPipe<S, E> {

    protected ExecutorService executorService;
    protected int threadNumber = 10;
    protected static int futureQueueSize = 1000;
    protected Thread prefetchThread;

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    public void setExecutorService(ExecutorService es) {
        this.executorService = es;
    }

    protected <T> void checkThreadInit(FutureQueue<T> futureQueue) {
        if (this.executorService == null) {
            this.executorService = Executors.newCachedThreadPool();
        }
        if (this.prefetchThread == null) {
            this.prefetchThread = new Thread(new PrefetchThread<T>(futureQueue));
        }
        if (!this.prefetchThread.isAlive() && starts.hasNext()) {
            try {
                this.prefetchThread.start();
            } catch (Exception e) {
                this.prefetchThread = new Thread(new PrefetchThread<T>(futureQueue));
                this.prefetchThread.start();
            }
        }
    }

    //Returns the number of active threads in executorService
    private int getThreadCount() {
        return ((ThreadPoolExecutor)this.executorService).getActiveCount();
    }

    protected abstract <T> Callable<T> createNewCall(S s, FutureQueue<T> futureQueue);

    //Checks whether this Pipe emit all the values.
    protected <T> boolean isEnded(FutureQueue<T> futureQueue) {
        return !this.prefetchThread.isAlive() && !futureQueue.hasNextFuture();
    }

    protected <T> void notifyPrefetch(FutureQueue<T> futureQueue) {
        synchronized (this.prefetchThread) {
            if (futureQueue.needAdd() && this.prefetchThread.isAlive()) {
                this.prefetchThread.notify();
            }
        }
    }

    //The prefetch thread will be blocked when the future queue do not need a new element.
    //It will be notified by processNextStart() when the future queue do not have enough element.
    protected class PrefetchThread<T> implements Runnable {

        public FutureQueue<T> futureQueue;

        public PrefetchThread(FutureQueue<T> q) {
            this.futureQueue = q;
        }

        @Override
        public void run() {
            try {
                while (starts.hasNext()) {
                    synchronized (this) {
                        while (!futureQueue.needAdd()) {
                            wait();
                        }
                    }
                    while (starts.hasNext() && futureQueue.canAdd()) {
                        S s = starts.next();
                        while (true) {
                            if (threadNumber > getThreadCount()) {
                                Future<T> future = executorService.submit(createNewCall(s, futureQueue));
                                futureQueue.addFuture(future);
                                break;
                            }
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}
