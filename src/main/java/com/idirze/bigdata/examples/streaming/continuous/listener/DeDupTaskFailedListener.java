package com.idirze.bigdata.examples.streaming.continuous.listener;

import org.apache.spark.TaskContext;
import org.apache.spark.util.TaskFailureListener;

import java.io.Serializable;

public class DeDupTaskFailedListener implements TaskFailureListener, Serializable {

    @Override
    public void onTaskFailure(TaskContext context, Throwable error) {

    }

}
