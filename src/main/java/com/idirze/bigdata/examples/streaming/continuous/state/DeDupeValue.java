package com.idirze.bigdata.examples.streaming.continuous.state;

import lombok.Builder;
import lombok.Data;

import java.io.Serializable;

@Builder
@Data
public class DeDupeValue implements Serializable {

    // serves for audit
    //topic name,
    // partition, offset, nb attemeps, failure
}
