package com.idirze.bigdata.examples.streaming.continuous.state;

import com.idirze.bigdata.examples.streaming.continuous.DeDupeStream;
import com.idirze.bigdata.examples.streaming.continuous.KafkaUtilsBaseTest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import scala.Serializable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import static org.apache.spark.sql.streaming.OutputMode.Append;
import static org.assertj.core.api.Assertions.assertThat;

public class StreamingDeDupTest extends KafkaUtilsBaseTest implements Serializable {

    private static final String TOPIC_IN_SPARSE = "topic-in-sparse-v1";
    private static final String TOPIC_IN_IMMEDIATE = "topic-in-immediate-v1";
    private static SparkSession spark;

    @BeforeAll
    public static void setUpClass() {
        spark = SparkSession
                .builder()
                .appName("ContinuousStreamingDedupTest")
                .master("local[*]")
                .config("spark.sql.shuffle.partitions", "100")
                .config("spark.task.cpus", 1) // Important

                .getOrCreate();

        // Create the input topic
        getKafkaTestUtils().createTopic(TOPIC_IN_SPARSE, 3, (short) 1);
        // Create the input topic
        getKafkaTestUtils().createTopic(TOPIC_IN_IMMEDIATE, 3, (short) 1);

    }

    @Test
    @DisplayName("Ensure the random duplicates in the stream are removed from the output")
    public void random_duplicates_should_be_removed_test() {

        int nbRecords = 100;//1000;
        int nbDuplicates = 24;//245;

        // Insert some smoke data into the input kafka topic:
        // #nbRecords + nbDuplicates records into the input topic
        produceSmokeDataWithRandomDuplicates(nbRecords, nbDuplicates, TOPIC_IN_SPARSE, "key", "value");

        // Create an input stream from the input kafka topic
        Dataset<Row> inStream = createStreamingDataFrame(TOPIC_IN_SPARSE);
        List<Row> inResult = collectAsList(inStream, "InputStreamN");

        assertThat(inResult.size()).
                isEqualTo(nbRecords + nbDuplicates);

        // Deduplicate the stream
        Dataset<Row> outStream = new DeDupeStream()
                .dropDuplicates(inStream, 100, "value");
        List<Row> result = collectAsList(outStream, "InputStreamM");

        List<Row> expected = new ArrayList<>();

        IntStream
                .range(0, nbRecords)
                .forEach(i -> expected.add(RowFactory.create("key" + i, "value" + i)));

        assertThat(result.size()).
                isEqualTo(expected.size());

        assertThat(result)
                .containsExactlyInAnyOrder(expected.toArray(new Row[result.size()]));


    }

    @Test
    @DisplayName("Ensure the duplicates that are inserted immediately are removed from the output")
    public void immediate_duplicates_should_be_removed_test() {

        int nbRecords = 100;//1000;
        int nbImmDuplicates = 1;

        // Insert some smoke date into the input kafka topic:
        // #nbRecords + nbDuplicates records into the input topic
        // For each record sent, send immeditatelly nbImmDuplicates records
        produceSmokeDataWithConsecutiveDuplicates(nbRecords, nbImmDuplicates, TOPIC_IN_IMMEDIATE, "key", "value");

        // Create an input stream from the input kafka topic
        Dataset<Row> inStream = createStreamingDataFrame(TOPIC_IN_IMMEDIATE);
        List<Row> inResult = collectAsList(inStream, "InputStreamImmediate");

        assertThat(inResult.size()).
                isEqualTo(nbRecords + nbImmDuplicates * nbRecords);

        // Deduplicate the stream
        Dataset<Row> outStream = DeDupeStream.dropDuplicates(inStream, 100, "value");

        // Collect the result
        List<Row> result = collectAsList(outStream, "OutputStreamImmediate");

        List<Row> expected = new ArrayList<>();

        IntStream
                .range(0, nbRecords)
                .forEach(i -> expected.add(RowFactory.create("key" + i, "value" + i)));

        assertThat(result.size()).
                isEqualTo(expected.size());

        assertThat(result)
                .containsExactlyInAnyOrder(expected.toArray(new Row[result.size()]));

    }


    private List<Row> collectAsList(Dataset<Row> stream, String output) {
        stream
                // Collect the result in memory for testing
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("memory")
                .queryName(output)
                .outputMode(Append())
                .start()

                .processAllAvailable();

        return spark.sql("select * from " + output).collectAsList();
    }


    private Dataset<Row> createStreamingDataFrame(String topic) {

        return spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", getKafkaConnectString())
                .option("failOnDataLoss", "true") // Important
                .option("fetchOffset.numRetries", "3")
                .option("startingOffsets", "earliest")
                // 10 offsets per batch
                .option("maxOffsetsPerTrigger", "100")
                .option("kafkaConsumer.pollTimeoutMs", "1000")
                .option("subscribe", topic)
                .option("value.converter.schemas.enable", "false")
                .load();
    }

}
