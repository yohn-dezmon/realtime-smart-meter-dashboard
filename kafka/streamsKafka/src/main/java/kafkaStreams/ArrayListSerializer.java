package kafkaStreams;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public class ArrayListSerializer<T> implements Serializer<ArrayList<T>> {

    //    private final Comparator<T> comparator;
    private final Serializer<T> inner;

    public ArrayListSerializer(Serializer<T> inner) {
        this.inner = inner;

    }
    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // do nothing
    }

    @Override
    public byte[] serialize(final String topic, final ArrayList<T> queue) {
        final int size = queue.size();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);
        final Iterator<T> iterator = queue.iterator();
        try {
            out.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = inner.serialize(topic, iterator.next());
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            out.close();
        } catch (final IOException e) {
            throw new RuntimeException("unable to serialize PriorityQueue", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {

    }

}