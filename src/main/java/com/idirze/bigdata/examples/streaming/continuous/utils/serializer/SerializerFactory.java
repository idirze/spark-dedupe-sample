package com.idirze.bigdata.examples.streaming.continuous.utils.serializer;

public class SerializerFactory {

    public enum Type {
        BYTE_BUFFER
    }

    public static Serializer createSerailzer(SerializerFactory.Type type) {
        switch (type) {
            case BYTE_BUFFER:
                return new ByteBufferSerializer();
            default:
                throw new IllegalArgumentException("Unknown serializer type: " + type);
        }
    }
}
