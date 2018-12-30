package beamtest.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class ParquetWordCount {
    static class DebugPrint extends DoFn<GenericRecord, GenericRecord> {
        private static final Logger LOG = LoggerFactory.getLogger(DebugPrint.class);

        @ProcessElement
        public void process(ProcessContext ctx) {
            GenericRecord elem = ctx.element();
            LOG.info("Matched: " + elem);
            ctx.output(elem);
        }

    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }


    static class CountName extends PTransform<PCollection<GenericRecord>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<GenericRecord> input) {
            return input.apply(MapElements.into(TypeDescriptors.strings())
                        .via(w -> w.get("name").toString()))
                    .apply(Filter.by(w -> !w.isEmpty()))
                    .apply(Count.perElement());
        }
    }

    public interface WordCountOptions extends PipelineOptions {
        @Description("Path of the file to read from")
        @Default.String("people.parquet")
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
        Schema schema = null;
        try (InputStream is = new FileInputStream("people.avsc")) {
            schema = new Schema.Parser().parse(is);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        p.apply(FileIO.match().filepattern(options.getInputFile()))
                .apply(FileIO.readMatches())
                .apply(ParquetIO.readFiles(schema))
                .apply(ParDo.of(new DebugPrint()))
                .apply(new CountName())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply(TextIO.write().to(options.getOutput()));

        p.run().waitUntilFinish();
        System.out.println("waiting for result");
    }
}
