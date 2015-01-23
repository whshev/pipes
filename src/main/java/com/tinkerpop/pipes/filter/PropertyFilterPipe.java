package com.tinkerpop.pipes.filter;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * The PropertyFilterPipe either allows or disallows all Elements that have the provided value for a particular key.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterPipe<S extends Element, T> extends AsyncPipe<S, S> implements FilterPipe<S> {

    private final String key;
    private final Object value;
    private final Predicate predicate;

    public PropertyFilterPipe(final String key, final Predicate predicate, final Object value) {
        this.key = key;
        this.value = value;
        this.predicate = predicate;
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
            if (predicate.evaluate(s.getProperty(key), value)) {
                return this.s;
            } else {
                return null;
            }
        }

    }

    public String toString() {
        return PipeHelper.makePipeString(this, this.key, this.predicate, this.value);
    }

    public String getKey() {
        return this.key;
    }

    public Object getValue() {
        return this.value;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }
}
