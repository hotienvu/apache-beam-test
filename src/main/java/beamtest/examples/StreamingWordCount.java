package beamtest.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;

public class StreamingWordCount {

  public interface StreamingWordCountOptions extends PipelineOptions {

    @Description("Kafka topic to read from")
    @Default.String("test")
    @Validation.Required
    String getInputTopic();

    void setInputTopic(String topic);

    @Description("File to output to")
    @Default.String("streaming_wordcounts")
    @Validation.Required
    String getOutputFile();

    void setOutputFile(String outputFile);
  }

  public static void main(String[] args) {
    StreamingWordCountOptions options = PipelineOptionsFactory.fromArgs(args)
      .withValidation().as(StreamingWordCountOptions.class);

    Pipeline pipeline = Pipeline.create(options);


    pipeline
      .apply(KafkaIO.<Long, String>read()
        .withBootstrapServers("localhost:9092")
        .withTopic(options.getInputTopic())
        .withKeyDeserializer(LongDeserializer.class)
        .withValueDeserializer(StringDeserializer.class)
      )
      .apply(MapElements.into(TypeDescriptors.strings()).via(input -> input.getKV().getValue()))
      .apply(Window.into(SlidingWindows.of(Duration.standardSeconds(60)).every(Duration.standardSeconds(5))))
      .apply(new MinimalWordCount.CountWords())
      .apply(MapElements.into(TypeDescriptors.strings()).via(kv -> kv.getValue() + " "  + kv.getKey()))
      .apply(KafkaIO.<Long, String>write()
        .withTopic("test-output")
        .withBootstrapServers("localhost:9092")
        .withKeySerializer(LongSerializer.class)
        .withValueSerializer(StringSerializer.class)
        .values()
      );

    pipeline.run().waitUntilFinish();
  }
}
