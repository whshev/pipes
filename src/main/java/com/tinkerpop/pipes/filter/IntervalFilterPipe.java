package com.tinkerpop.pipes.filter;

import com.tinkerpop.blueprints.Compare;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * IntervalFilterPipe will filter an element flowing through it according to whether a particular property value of the element is within provided range.
 * For those objects who property value for provided key is null, the element is filtered out of the stream.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class IntervalFilterPipe<S extends Element> extends AsyncPipe<S, S> implements FilterPipe<S> {

    private final String key;
    private final Comparable startValue;
    private final Comparable endValue;

    public IntervalFilterPipe(final String key, final Comparable startValue, final Comparable endValue) {
        this.key = key;
        this.startValue = startValue;
        this.endValue = endValue;
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

    public String getKey() {
        return this.key;
    }

    public Comparable getStartValue() {
        return this.startValue;
    }

    public Comparable getEndValue() {
        return this.endValue;
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
            final Object value = s.getProperty(key);
            if (null == value)
                return null;
            else if (Compare.GREATER_THAN_EQUAL.evaluate(value, startValue) && Compare.LESS_THAN.evaluate(value, endValue)) {
                return this.s;
            }
            return null;
        }

    }

}
