package kafkaStreams;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;

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
        final String comma = ",";
        final byte[] commabytes = comma.getBytes();
        final int commabyteslen = commabytes.length;
        try {
            out.writeInt(size);
            while (iterator.hasNext()) {
                final byte[] bytes = inner.serialize(topic, iterator.next());
                byte[] c = new byte[bytes.length + commabyteslen];
                System.arraycopy(bytes, 0, c, 0, bytes.length);
                System.arraycopy(commabytes, 0, c, bytes.length, commabytes.length);
                out.writeInt(c.length);
                out.write(c);
            }
            out.close();
        } catch (final IOException e) {
            throw new RuntimeException("unable to serialize ArrayList", e);
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {

    }

}