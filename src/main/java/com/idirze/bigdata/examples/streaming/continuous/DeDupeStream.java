package com.idirze.bigdata.examples.streaming.continuous;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;

public class DeDupeStream implements Serializable {


    public static Dataset<Row> dropDuplicates(Dataset<Row> stream,
                                              String deDupeKey) {
        return mapPartitions(stream.repartition(col(deDupeKey)), deDupeKey);

    }

    public static Dataset<Row> dropDuplicates(Dataset<Row> stream,
                                              int partitions,
                                              String deDupeKey) {

        return mapPartitions(stream
                        // clusterBy ==>
                        .repartition(partitions, col(deDupeKey)), deDupeKey
                // .sortWithinPartitions(col("value"))
        );


    }


    private static Dataset<Row> mapPartitions(Dataset<Row> stream,
                                              String deDupeKey) {


        ExpressionEncoder<Row> encoder = RowEncoder.apply(stream.schema());
        DeDupePartition deDupPartition = new DeDupePartition(deDupeKey);

        return stream
                .mapPartitions(deDupPartition, encoder);
    }
}
