package com.tinkerpop.pipes.filter;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * The PropertyFilterPipe either allows or disallows all Elements that have the provided value for a particular key.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyFilterPipe<S extends Element, T> extends AsyncPipe<S, S> implements FilterPipe<S> {

    private final String key;
    private final Object value;
    private final Predicate predicate;

    //Added by whshev.
    protected FutureQueue<S> futureQueue = new FutureQueue<S>(futureQueueSize);

    public PropertyFilterPipe(final String key, final Predicate predicate, final Object value) {
        this.key = key;
        this.value = value;
        this.predicate = predicate;
    }

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
            if (predicate.evaluate(s.getProperty(key), value)) {
                return s;
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
