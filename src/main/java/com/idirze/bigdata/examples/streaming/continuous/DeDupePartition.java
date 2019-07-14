package com.idirze.bigdata.examples.streaming.continuous;

import com.idirze.bigdata.examples.streaming.continuous.listener.DeDupTaskFailedListener;
import com.idirze.bigdata.examples.streaming.continuous.listener.DuplicateFoundListener;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStore;
import com.idirze.bigdata.examples.streaming.continuous.state.DeDupeValue;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@Slf4j
public class DeDupePartition implements MapPartitionsFunction<Row, Row> {


    private CustomStateStore store;
    private String deDupeKey;
    private DuplicateFoundListener duplicateFoundListener;

    public DeDupePartition(String deDupeKey) {
        this.store = new CustomStateStore();
        this.deDupeKey = deDupeKey;
        this.duplicateFoundListener = new DuplicateFoundListener();
    }

    @Override
    public Iterator<Row> call(Iterator<Row> it) throws Exception {
        TaskContext taskContext = TaskContext.get();

        long taskId = taskContext.taskAttemptId();
        long partitionId = taskContext.partitionId();

        if (log.isDebugEnabled()) {
            log.debug("Task {} with Partition {} started", taskId, partitionId);
        }

        taskContext.addTaskFailureListener(new DeDupTaskFailedListener());

        int count = 0;
        List<Row> deuplicatedRows = new ArrayList<>();

        while (it.hasNext()) {
            Row row = it.next();
            count++;
            byte[] key = row.getAs(deDupeKey);
            if (!store.exists(key)) {
                deuplicatedRows.add(row);
                /**
                 * Use {@link DeDupeValue} for the value
                 */
                store.put(key, "1".getBytes());
            } else {
                // Audit your duplicates
                duplicateFoundListener.onDuplicateFound(TaskContext.get(), key);
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("Task {} with Partition {} completed. Total rows count: {}, Total count after deduplication: {}", taskId, partitionId, count, deuplicatedRows.size());
        }
        return deuplicatedRows.iterator();
    }

}
