package com.idirze.bigdata.examples.streaming.continuous.constants;

import com.google.common.base.Strings;
import com.idirze.bigdata.examples.streaming.continuous.state.memory.MemoryStateStoreBackend;

import static java.util.Arrays.asList;

public enum StateStoreBackendType {

    MEMORY() {
        /**
         * Memory backend implementation class
         */
        @Override
        public String backendClass() {
            return MemoryStateStoreBackend.class.getCanonicalName();
        }
    };

    public abstract String backendClass();

    public static StateStoreBackendType backendOf(String backend) {
        try {
          return   StateStoreBackendType.valueOf(Strings.nullToEmpty(backend).trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            // log correctly
            throw new IllegalArgumentException("Unknown state backend: " + backend + ", possible values: " + asList(StateStoreBackendType.values()));
        }
    }

}
