package com.tinkerpop.pipes.filter;

import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

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
        this.futureQueue = new FutureQueue<S>(FUTURE_QUEUE_SIZE);
    }

    //Modified by whshev.
    protected S processNextStart() {
        checkThreadInit();
        while (true) {
            while (this.futureQueue.hasNextFuture()) {
                Future<S> future = this.futureQueue.getNextFuture();
                notifyPrefetch();
                try {
                    S value = null;
                    if (pathEnabled) {
                        List path = this.futureQueue.getNextPath();
                        value = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (value != null) {
                            this.currentPath = path;
                            return value;
                        }
                    } else {
                        value = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (value != null) {
                            return value;
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    future.cancel(true);
                }
            }
            notifyPrefetch();
            if (isEnded()) throw new NoSuchElementException();
        }
    }

    //Added by whshev.
    @Override
    protected Callable createNewCall(S s) {
        return new Calculator(s);
    }

    //Added by whshev.
    public class Calculator implements Callable<S> {

        private S s;

        public Calculator(S s) {
            this.s = s;
        }

        public S call() throws Exception {
            if (filterFunction.compute(s)) {
                return this.s;
            } else {
                return null;
            }
        }

    }
}