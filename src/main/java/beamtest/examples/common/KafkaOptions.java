package beamtest.examples.common;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaOptions extends PipelineOptions {

  @Description("Kafka topic to read from")
  @Default.String("test")
  @Validation.Required
  String getInputTopic();

  void setInputTopic(String topic);

  @Description("Kafka topic to write output to")
  @Default.String("test-output")
  @Validation.Required
  String getOutputTopic();

  void setOutputTopic(String outputTopic);
}

