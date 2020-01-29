package kafkaStreams;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

// PriorityQueueDeserializer<T> implements Deserializer<PriorityQueue<T>>

public class ArrayListDeserializer<T> implements Deserializer<ArrayList<T>> {

//    private final Comparator<T> comparator;
    private final Deserializer<T> inner;

    public ArrayListDeserializer(Deserializer<T> inner) {

        this.inner = inner;
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        // do nothing
    }

    @Override
    public ArrayList<T> deserialize(final String s, final byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        final ArrayList<T> arrayList = new ArrayList<>();
        // i took out comparator from ArrayList<>()... may need to add it elsewhere....
        final DataInputStream dataInputStream = new DataInputStream(new ByteArrayInputStream(bytes));
        try {
            final int records = dataInputStream.readInt();
            for (int i = 0; i < records; i++) {
                final byte[] valueBytes = new byte[dataInputStream.readInt()];
                dataInputStream.read(valueBytes);
                arrayList.add(inner.deserialize(s, valueBytes));
            }
        } catch (final IOException e) {
            throw new RuntimeException("Unable to deserialize ArrayList", e);
        }
        return arrayList;
    }

    @Override
    public void close() {

    }
}
