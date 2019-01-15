package beamtest.examples;

import beamtest.examples.common.KafkaIOs;
import beamtest.examples.common.KafkaOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class StreamingWordCount {

  private static final Logger LOG = LoggerFactory.getLogger(StreamingWordCount.class);


  public static void main(String[] args) {
    KafkaOptions options = PipelineOptionsFactory.fromArgs(args)
      .withValidation().as(KafkaOptions.class);

    Pipeline pipeline = Pipeline.create(options);
    Long maxTimestamp = Instant.now().getMillis();
    Long minTimestamp = maxTimestamp - Duration.standardSeconds(10).getMillis();

    pipeline
      .apply(KafkaIOs.readFromKafka(options.getInputTopic(), "localhost:9092"))
      .apply(MapElements.into(TypeDescriptors.strings()).via(input -> input.getKV().getValue()))
      .apply(ParDo.of(new DoFn<String, String>() {
        @ProcessElement
        public void process(ProcessContext ctx) {
          Instant randomTimestamp = new Instant(ThreadLocalRandom.current()
            .nextLong(minTimestamp, maxTimestamp));
          String[] elems= ctx.element().split(",", 2);
          if (elems.length == 2) {
            Instant timestamp = new Instant(Long.parseLong(elems[0]));
            LOG.info("{}, {}, {}", timestamp, ctx.timestamp(), elems[1]);
            ctx.output(elems[1]);
          } else {
            ctx.output(ctx.element());
          }
        }
      }))
      .apply(Window
        .<String>into(SlidingWindows.of(Duration.standardHours(1)).every(Duration.standardMinutes(10)))
        .withAllowedLateness(Duration.standardMinutes(5))
        .triggering(AfterWatermark.pastEndOfWindow()
          .withEarlyFirings(AfterPane.elementCountAtLeast(100))
          .withLateFirings(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardMinutes(1)))
        )
        .accumulatingFiredPanes()
      )

      .apply(new MinimalWordCount.CountWords())
      .apply(MapElements.via(new MinimalWordCount.FormatAsTextFn()))
      .apply(KafkaIOs.writeToKafka(options.getOutputTopic(), "localhost:9092"));
    pipeline.run().waitUntilFinish();
  }
}
