package kafkaStreams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Map;

public class ArrayListSerde<T> implements Serde<ArrayList<T>> {

    private final Serde<ArrayList<T>> inner;

    public ArrayListSerde(final Serde<T> serde) {
        inner =
                Serdes.serdeFrom(
                        new ArrayListSerializer<>(serde.serializer()),
                        new ArrayListDeserializer<>(serde.deserializer()));
    };
    // final Comparator<T> comparator, final Serde<T> avroSerde) {
    //        inner = Serdes.serdeFrom(new ArrayListSerializer<>(comparator, avroSerde.serializer()),
    //                new ArrayListDeserializer<>(comparator, avroSerde.deserializer())

    @Override
    public Serializer<ArrayList<T>> serializer() {
        return inner.serializer();
    }

    @Override
    public Deserializer<ArrayList<T>> deserializer() {
        return inner.deserializer();
    }

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
        inner.serializer().configure(configs, isKey);
        inner.deserializer().configure(configs, isKey);
    }

    @Override
    public void close() {
        inner.serializer().close();
        inner.deserializer().close();
    }


}
