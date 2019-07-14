package com.idirze.bigdata.examples.streaming.continuous.state.memory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.idirze.bigdata.examples.streaming.continuous.state.CustomStateStoreBackend;
import com.idirze.bigdata.examples.streaming.continuous.state.DeDupeKey;
import com.idirze.bigdata.examples.streaming.continuous.utils.serializer.Serializer;
import com.idirze.bigdata.examples.streaming.continuous.utils.serializer.SerializerFactory;
import com.idirze.bigdata.examples.streaming.continuous.utils.serializer.SerializerFactory.Type;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.spark.TaskContext;
import scala.collection.Iterator;

import java.nio.ByteBuffer;
import java.util.Map;

import static java.nio.ByteBuffer.wrap;
import static java.util.concurrent.TimeUnit.MINUTES;

@Slf4j
public class MemoryStateStoreBackend implements CustomStateStoreBackend {


    private /* Shared or One cache instance by executor */ Cache<ByteBuffer, byte[]> store;
    private MemoryStateStoreIterator iterator;
    private Serializer<ByteBuffer> keySerializer;

    public MemoryStateStoreBackend() {

        this.store = CacheBuilder
                .newBuilder()
                .expireAfterWrite(15, MINUTES)
                .build();

        this.iterator = new MemoryStateStoreIterator(this.store);
        this.keySerializer = SerializerFactory.createSerailzer(Type.BYTE_BUFFER);
    }

    @Override
    public byte[] get(byte[] key) {
        // To prevent concurrency issues, we suffix operatorId and partitionId for each key
        //spark.task.cpus => 1

        byte[] valueBytes = findByPrefix(key);

        if (valueBytes == null) {
            // Publish the data to the sink
            log.debug("DDE - Value not yet seen");
            return null;
        }

        // Send the duplicates into an audit topic
        // - In case of failure or retry the data will may not be pudblished to the sink
        // - Set the duplicate score:
        //   You can distinguish between normal duplicates & technical ones by versionning (current version < stored currentVersion  => retry)
        log.debug("DDE - Value already seen and maybe sent!");

        return valueBytes;
    }

    @Override
    public byte[] findByPrefix(byte[] key) {

        ByteBuffer keyByteBuffer = wrap(key);

        Iterator<Map.Entry<ByteBuffer, byte[]>> it = iterator.newIterator();

        byte[] valueBytes = null;

        while (it.hasNext()) {
            Map.Entry<ByteBuffer, byte[]>  next = it.next();
            if (keyByteBuffer.equals(wrap(keySerializer.deserialize(next.getKey()).getData()))) {
                valueBytes = store.getIfPresent(next.getKey());
                break;
            }
        }

        return valueBytes;
    }


    @Override
    public void put(byte[] key, byte[] value) {
        // The key, value may be reused, copy them
        byte[] keyCopy = SerializationUtils.clone(key);
        byte[] valueCopy = SerializationUtils.clone(value);

        TaskContext taskContext = TaskContext.get();

        DeDupeKey dedupeKey = DeDupeKey
                .builder()
                .data(keyCopy)
                .partitionId(taskContext.partitionId())
                .build();

        store.put(keySerializer.serialize(dedupeKey),
                valueCopy);
    }

    @Override
    public void remove(byte[] key) {
        store.invalidate(wrap(key));
    }

    @Override
    public boolean exist(byte[] key) {
        return get(key) != null;
    }


}
