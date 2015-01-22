package com.tinkerpop.pipes.filter;

import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.PipeFunction;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
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
        this.futureQueue = new FutureQueue<Map.Entry<S, List>>(FUTURE_QUEUE_SIZE);
    }

    //Modified by whshev.
    protected S processNextStart() {
        checkThreadInit();
        while (true) {
            notifyPrefetch();
            while (this.futureQueue.hasNextFuture()) {
                Future<Map.Entry<S, List>> future = this.futureQueue.getNextFuture();
                Map.Entry<S, List> me = null;
                try {
                    me = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                    future.cancel(true);
                }
                notifyPrefetch();
                if (me != null && me.getKey() != null) {
                    this.currentPath = me.getValue();
                    return me.getKey();
                }
            }
            if (isEnded()) throw new NoSuchElementException();
        }
    }

    //Added by whshev.
    @Override
    protected Callable createNewCall(S s, List path) {
        return new Calculator(s, path);
    }

    //Added by whshev.
    public class Calculator implements Callable<Map.Entry<S, List>> {

        private S s;
        private List p;

        public Calculator(S s, List path) {
            this.s = s;
            this.p = path;
        }

        public Map.Entry<S, List> call() throws Exception {
            if (filterFunction.compute(s)) {
                return new AbstractMap.SimpleEntry<S, List>(this.s, this.p);
            } else {
                return new AbstractMap.SimpleEntry<S, List>(null, this.p);
            }
        }

    }
}