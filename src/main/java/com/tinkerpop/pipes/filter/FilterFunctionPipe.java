package com.tinkerpop.pipes.filter;

import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * FilterFunctionPipe is a generic filter pipe.
 * It takes a PipeFunction that returns a Boolean for its compute() step. The argument is the next start of the pipe.
 * If the return is true, then start is emitted. Otherwise, the start is not emitted.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FilterFunctionPipe<S> extends AsyncPipe<S, S> implements FilterPipe<S> {

    private final PipeFunction<S, Boolean> filterFunction;

    public FilterFunctionPipe(final PipeFunction<S, Boolean> filterFunction) {
        this.filterFunction = filterFunction;
    }

    //Added by whshev.
    protected FutureQueue<S> futureQueue = new FutureQueue<S>(futureQueueSize);

    //Modified by whshev.
    protected S processNextStart() {
        checkThreadInit(this.futureQueue);
        while (true) {
            notifyPrefetch(this.futureQueue);
            while (this.futureQueue.hasNextFuture()) {
                S value = null;
                try {
                    value = this.futureQueue.getNextFuture().get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                notifyPrefetch(this.futureQueue);
                if (value != null)
                    return value;
            }
            if (isEnded(this.futureQueue)) throw new NoSuchElementException();
        }
    }

    //Added by whshev.
    @Override
    protected <T> Callable<T> createNewCall(S s, FutureQueue<T> futureQueue) {
        return (Callable<T>) new Calculator(s);
    }

    //Added by whshev.
    public class Calculator implements Callable<S> {

        private S s;

        public Calculator(S s) {
            this.s = s;
        }

        public S call() throws Exception {
            if (filterFunction.compute(s)) {
                return s;
            } else {
                return null;
            }
        }

    }
}