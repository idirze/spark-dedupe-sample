package com.idirze.bigdata.examples.streaming.continuous.utils;

import com.idirze.bigdata.examples.streaming.continuous.exception.StateStoreBackendInstantiationException;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStoreBackend;
import org.apache.spark.sql.execution.streaming.state.StateStoreConf;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class StateStoreUtils {

    private static Class[] argsClass = new Class[]{};

    public static CustomStateStoreBackend createStateStoreBackand(String stateStoreClass, Object... args) {

        try {
            Class clazz = Class.forName(stateStoreClass);
            Constructor constructor = clazz.getConstructor(argsClass);
            return (CustomStateStoreBackend) constructor.newInstance(args);
        } catch (IllegalAccessException | InvocationTargetException | InstantiationException | ClassNotFoundException | NoSuchMethodException e) {
            throw new StateStoreBackendInstantiationException("Failed to instantiate stateStore class: " + stateStoreClass, e);
        }
    }

}
