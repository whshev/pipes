package com.tinkerpop.pipes.transform;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.pipes.AsyncPipe;
import com.tinkerpop.pipes.util.PipeHelper;
import com.tinkerpop.pipes.util.structures.FutureQueue;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
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
        this.futureQueue = new FutureQueue<Map.Entry<E, List>>(FUTURE_QUEUE_SIZE);
    }

    public PropertyPipe(final String key, final boolean allowNull) {
        this.key = key;
        this.allowNull = allowNull;
        this.futureQueue = new FutureQueue<Map.Entry<E, List>>(FUTURE_QUEUE_SIZE);
    }

    //Modified by whshev.
    protected E processNextStart() {
        checkThreadInit();
        while (true) {
            notifyPrefetch();
            while (this.futureQueue.hasNextFuture()) {
                Future<Map.Entry<E, List>> future = this.futureQueue.getNextFuture();
                Map.Entry<E, List> me = null;
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
                if (me != null && (this.allowNull || me.getKey() != null)) {
                    this.currentPath = me.getValue();
                    return me.getKey();
                }
            }
            if (isEnded()) throw new NoSuchElementException();
        }
    }

    //Added by whshev.
    @Override
    public Callable createNewCall(S s, List path) {
        return new Calculator(s, path);
    }

    //Added by whshev.
    public class Calculator implements Callable<Map.Entry<E, List>> {

        private Element e;
        private List p;

        public Calculator(Element e, List path) {
            this.e = e;
            this.p = path;
        }

        public Map.Entry<E, List> call() throws Exception {
            return new AbstractMap.SimpleEntry<E, List>((E) e.getProperty(key), this.p);
        }

    }

    public String toString() {
        return PipeHelper.makePipeString(this, this.key);
    }
}
