package com.idirze.bigdata.examples.streaming.continuous.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.TaskContext;

import java.io.Serializable;

@Slf4j
public class DuplicateFoundListener implements Serializable {

    public void onDuplicateFound(TaskContext context, byte[] key) {
        log.debug("Duplicate found");
    }

}
