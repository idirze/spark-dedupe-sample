package com.idirze.bigdata.examples.streaming.continuous.utils.serializer;

import com.idirze.bigdata.examples.streaming.continuous.state.DeDupeKey;

import java.io.Serializable;

public interface Serializer<T> extends Serializable {

    T serialize(DeDupeKey data);

    DeDupeKey deserialize(T data);
}
