package com.idirze.bigdata.examples.streaming.continuous.utils.serializer;

import com.idirze.bigdata.examples.streaming.continuous.state.DeDupeKey;
import org.apache.commons.lang3.SerializationUtils;

import java.nio.ByteBuffer;

import static java.nio.ByteBuffer.wrap;

public class ByteBufferSerializer implements Serializer<ByteBuffer> {

    @Override
    public ByteBuffer serialize(DeDupeKey key) {
        return wrap(SerializationUtils.serialize(key));

    }

    @Override
    public DeDupeKey deserialize(ByteBuffer data) {
        return SerializationUtils.deserialize(data.array());
    }

}
