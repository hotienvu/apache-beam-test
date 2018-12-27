package beamtest.examples;

import beamtest.examples.common.WriteOneFilePerWindow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowedWordCount {
    private static final int WINDOW_SIZE = 10;

    static class AddTimestampFn extends DoFn<String, String> {

        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        public AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void process(ProcessContext ctx) {
            Instant randomTimestamp = new Instant(ThreadLocalRandom.current()
                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));
            ctx.outputWithTimestamp(ctx.element(), randomTimestamp);
        }
    }

    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }
    }


    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(Options.class).getMinTimestampMillis()
                    + Duration.standardHours(1).getMillis();
        }
    }

    public interface Options
            extends MinimalWordCount.WordCountOptions {
        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneHour.class)
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long value);

        @Description("Fixed number of shards to produce per window")
        Integer getNumShards();

        void setNumShards(Integer numShards);
    }

    private static void runWindowedWordCount(Options options) throws IOException {
        final String output = options.getOutput();
        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());

        Pipeline pipeline = Pipeline.create(options);

        /*
         * Concept #1: the Beam SDK lets us run the same pipeline with either a bounded or
         * unbounded input source.
         */
        PCollection<String> input =
                pipeline
                        /* Read from the GCS file. */
                        .apply(TextIO.read().from(options.getInputFile()))
                        // Concept #2: Add an element timestamp, using an artificial time just to show windowing.
                        // See AddTimestampFn for more detail on this.
                        .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

        /*
         * Concept #3: Window into fixed windows. The fixed window size for this example defaults to 1
         * minute (you can change this with a command-line option). See the documentation for more
         * information on how fixed windows work, and for information on the other types of windowing
         * available (e.g., sliding windows).
         */
        PCollection<String> windowedWords =
                input.apply(
                        Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

        /*
         * Concept #4: Re-use our existing CountWords transform that does not have knowledge of
         * windows over a PCollection containing windowed values.
         */
        PCollection<KV<String, Long>> wordCounts = windowedWords.apply(new MinimalWordCount.CountWords());

        /*
         * Concept #5: Format the results and write to a sharded file partitioned by window, using a
         * simple ParDo operation. Because there may be failures followed by retries, the
         * writes must be idempotent, but the details of writing to files is elided here.
         */
        wordCounts
                .apply(MapElements.via(new MinimalWordCount.FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(output, options.getNumShards()));

        PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception exc) {
            result.cancel();
        }
    }

    public static void main(String[] args) throws IOException {
        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runWindowedWordCount(options);
    }
}
