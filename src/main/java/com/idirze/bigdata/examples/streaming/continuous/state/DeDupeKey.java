package com.idirze.bigdata.examples.streaming.continuous.state;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;

@Builder
@Data
public class DeDupeKey implements Serializable {

    private byte[] data;
    private int partitionId;

}
