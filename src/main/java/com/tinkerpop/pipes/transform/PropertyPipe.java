package com.tinkerpop.pipes.transform;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.tinkerpop.pipes.util.structures.FutureQueue;

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

    //Added by whshev.
    protected FutureQueue<E> futureQueue = new FutureQueue<E>(futureQueueSize);

    public PropertyPipe(final String key) {
        this.key = key;
        this.allowNull = true;
    }

    public PropertyPipe(final String key, final boolean allowNull) {
        this.key = key;
        this.allowNull = allowNull;
    }

    //Modified by whshev.
    protected E processNextStart() {
        checkThreadInit(this.futureQueue);
        while (true) {
            notifyPrefetch(this.futureQueue);
            while (this.futureQueue.hasNextFuture()) {
                E value = null;
                try {
                    value = this.futureQueue.getNextFuture().get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                notifyPrefetch(this.futureQueue);
                if (this.allowNull || value != null)
                    return value;
            }
            if (isEnded(this.futureQueue)) throw new NoSuchElementException();
        }
    }

    //Added by whshev.
    @Override
    public <E> Callable<E> createNewCall(S s, FutureQueue<E> futureQueue) {
        return (Callable<E>) new Calculator(s);
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
