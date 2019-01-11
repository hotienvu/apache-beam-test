package beamtest.examples.common;

import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaIOs {

  public static PTransform<PBegin, PCollection<KafkaRecord<Long, String>>> readFromKafka(String topic, String bootstrapServers) {
    return KafkaIO.<Long, String>read()
      .withBootstrapServers(bootstrapServers)
      .withTopic(topic)
//      .withTimestampFn(new SerializableFunction<KV<Long, String>, Instant>() {
//        @Override
//        public Instant apply(KV<Long, String> input) {
//          LOG.info("{}", input.getValue());
//          String[] tks = input.getValue().split(",", 2);
//          return tks.length == 2 ? new Instant(Long.parseLong(tks[0])) : Instant.now();
//        }
//      })
      .withKeyDeserializer(LongDeserializer.class)
      .withValueDeserializer(StringDeserializer.class);
  }


  public static PTransform<PCollection<String>, PDone> writeToKafka(String topic, String bootstrapServers) {
    return KafkaIO.<Long, String>write()
      .withTopic(topic)
      .withBootstrapServers(bootstrapServers)
      .withKeySerializer(LongSerializer.class)
      .withValueSerializer(StringSerializer.class)
      .values();
  }


}
