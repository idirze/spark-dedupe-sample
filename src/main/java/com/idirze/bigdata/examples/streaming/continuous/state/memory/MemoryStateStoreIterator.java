package com.idirze.bigdata.examples.streaming.continuous.state.memory;

import com.google.common.cache.Cache;
import scala.collection.Iterator;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Map;

import static scala.collection.JavaConversions.asScalaIterator;

public class MemoryStateStoreIterator implements Serializable {

    private Cache<ByteBuffer, byte[]> store;

    public MemoryStateStoreIterator(Cache<ByteBuffer, byte[]> store) {
        this.store = store;

    }

    public Iterator<Map.Entry<ByteBuffer, byte[]>> newIterator() {
        return asScalaIterator(new java.util.Iterator<Map.Entry<ByteBuffer, byte[]>>() {

            private java.util.Iterator<Map.Entry<ByteBuffer, byte[]>> it = store.asMap().entrySet().iterator();

            @Override
            public boolean hasNext() {
                return it.hasNext();
            }

            @Override
            public Map.Entry<ByteBuffer, byte[]> next() {
                return it.next();

            }
        });
    }

}
