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
    protected static int FUTURE_QUEUE_SIZE = 1000;
    protected static int TIMEOUT_SECONDS = 10;
    protected PrefetchThread prefetchThread;
    protected FutureQueue futureQueue;

    public void setThreadNumber(int threadNumber) {
        this.threadNumber = threadNumber;
    }

    public void setExecutorService(ExecutorService es) {
        this.executorService = es;
    }

    protected void checkThreadInit() {
        if (this.executorService == null) {
            this.executorService = Executors.newCachedThreadPool();
        }
        if (this.prefetchThread == null) {
            this.prefetchThread = new PrefetchThread();
        }
        if (!this.prefetchThread.isAlive() && starts.hasNext()) {
            try {
                this.prefetchThread.start();
            } catch (Exception e) {
                e.printStackTrace();
                this.prefetchThread = new PrefetchThread();
                this.prefetchThread.start();
            }
        }
    }

    //Returns the number of active threads in executorService
    private int getThreadCount() {
        return ((ThreadPoolExecutor)this.executorService).getActiveCount();
    }

    protected abstract Callable createNewCall(S s);

    //Checks whether this Pipe emit all the values.
    protected boolean isEnded() {
        return !this.prefetchThread.isAlive() && !this.futureQueue.hasNextFuture() && !starts.hasNext();
    }

    protected void notifyPrefetch() {
        if (this.prefetchThread.isAlive()) {
            if (futureQueue.needAdd()) {
                synchronized (this.prefetchThread) {
                    this.prefetchThread.notify();
                }
            }
        } else {
            this.prefetchThread = new PrefetchThread();
            this.prefetchThread.start();
        }
    }

    //The prefetch thread will be blocked when the future queue do not need a new element.
    //It will be notified by processNextStart() when the future queue do not have enough element.
    protected class PrefetchThread extends Thread {

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
                                Future future = executorService.submit(createNewCall(s));
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
