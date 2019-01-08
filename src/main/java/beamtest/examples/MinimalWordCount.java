package beamtest.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MinimalWordCount {
    private static final String SPLIT_REGEX_PATTERN = "[\\s|/|:|_|,|.]";

    static class FilterWordByLengthFn extends DoFn<String, String> {
        private static final Logger LOG = LoggerFactory.getLogger(FilterWordByLengthFn.class);
        private int length;

        FilterWordByLengthFn(int length) {
            this.length = length;
        }

        @ProcessElement
        public void process(ProcessContext ctx) {
            String elem = ctx.element();
            if (elem.length() == length) {
                LOG.info("Matched: " + elem);
                ctx.output(elem);
            }
        }

    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }


    static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {
            return input.apply("Extract works", FlatMapElements.into(TypeDescriptors.strings())
                            .via(w -> Arrays.asList(w.split(SPLIT_REGEX_PATTERN))))
                    .apply(Filter.by(w -> !w.isEmpty()))
                    .apply(Count.perElement());
        }
    }

    interface WordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("file:///home/vho/work/github/beam/NOTICE")
        String getInputFile();

        void setInputFile(String value);

        /** Set this required option to specify where to write the output. */
        @Description("Path of the file to write to")
        @Validation.Required
        @Default.String("wordcounts")
        String getOutput();

        void setOutput(String value);
    }

    public static void main(String[] args) {
        WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(WordCountOptions.class);

        Pipeline p = Pipeline.create(options);
        // Read is a PTransform<PBegin, PCollection<String>>
        p.apply(TextIO.read().from(options.getInputFile()))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply(TextIO.write().to(options.getOutput()));

        p.run();
        System.out.println("waiting for result");
    }
}
