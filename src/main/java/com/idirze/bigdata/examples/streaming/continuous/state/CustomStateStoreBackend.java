package com.idirze.bigdata.examples.streaming.continuous.state;

import scala.Option;
import scala.collection.Iterator;

import java.io.Serializable;

public interface CustomStateStoreBackend extends Serializable {

    /**
     * Perform get operations on a single row.
     *
     * @param key
     * @return
     */
    byte[] get(byte[] key);

    /**
     * Perform get operations on a single row.
     *
     * @param key
     * @return
     */
    byte[] findByPrefix(byte[] key);


    /**
     * perform put operations for a single row.
     *
     * @param key
     * @param value
     */
    void put(byte[] key, byte[] value);

    /**
     * Delete an entire row
     *
     * @param key
     */
    void remove(byte[] key);


    /**
     * Check the provided key exists
     *
     * @param key
     * @return
     */
    boolean exist(byte[] key);

}
