package com.idirze.bigdata.examples.streaming.continuous.state;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import scala.Option;
import scala.collection.Iterator;

import java.io.Serializable;

import static com.idirze.bigdata.examples.streaming.continuous.constants.StateStoreBackendType.backendOf;
import static com.idirze.bigdata.examples.streaming.continuous.utils.StateStoreUtils.createStateStoreBackand;

public class CustomStateStore implements Serializable {

    private static CustomStateStoreBackend store;

    public CustomStateStore() {

        this.store = createStateStoreBackand(backendOf("memory")
                .backendClass());
    }


    public byte[] get(byte[] key) {
        return store.get(key);
    }

    public void put(byte[] key, byte[] value) {
        store.put(key, value);
    }

    public void remove(byte[] key) {
        store.remove(key);
    }

    public boolean exists(byte[] key) {
        return store.exist(key);
    }

}
