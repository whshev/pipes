package com.tinkerpop.pipes.transform;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.*;


/**
 * The PropertyPipe returns the property value of the Element identified by the provided key.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PropertyPipe<S extends Element, E> extends AsyncPipe<S, E> implements TransformPipe<S, E> {

    private final String key;
    private final boolean allowNull;

    public PropertyPipe(final String key) {
        this.key = key;
        this.allowNull = true;
        this.futureQueue = new FutureQueue<E>(FUTURE_QUEUE_SIZE);
    }

    public PropertyPipe(final String key, final boolean allowNull) {
        this.key = key;
        this.allowNull = allowNull;
        this.futureQueue = new FutureQueue<E>(FUTURE_QUEUE_SIZE);
    }

    //Modified by whshev.
    protected E processNextStart() {
        checkThreadInit();
        while (true) {
            while (this.futureQueue.hasNextFuture()) {
                Future<E> future = this.futureQueue.getNextFuture();
                notifyPrefetch();
                try {
                    E value = null;
                    if (pathEnabled) {
                        List path = this.futureQueue.getNextPath();
                        value = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (this.allowNull || value != null) {
                            this.currentPath = path;
                            return value;
                        }
                    } else {
                        value = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        if (this.allowNull || value != null) {
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
    public Callable createNewCall(S s) {
        return new Calculator(s);
    }

    //Added by whshev.
    public class Calculator implements Callable<E> {

        private Element e;

        public Calculator(Element e) {
            this.e = e;
        }

        public E call() throws Exception {
            return e.getProperty(key);
        }

    }

    public String toString() {
        return PipeHelper.makePipeString(this, this.key);
    }
}
