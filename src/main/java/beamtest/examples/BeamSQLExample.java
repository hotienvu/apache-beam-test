package beamtest.examples;


import beamtest.examples.common.AvroUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import javax.annotation.Nullable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


/**
 * This is a quick example, which uses Beam SQL DSL to create a data pipeline.
 *
 * <p>Run the example from the Beam source root with
 *
 * <pre>
 *   ./gradlew :beam-sdks-java-extensions-sql:runBasicExample
 * </pre>
 *
 * <p>The above command executes the example locally using direct runner. Running the pipeline in
 * other runners require additional setup and are out of scope of the SQL examples. Please consult
 * Beam documentation on how to run pipelines.
 */
class BeamSqlExample {
  private static org.apache.avro.Schema AVRO_SCHEMA;
  private static Schema BEAM_SCHEMA;

  static {
    try (
            InputStream is = new FileInputStream("people.avsc")) {
      AVRO_SCHEMA = new org.apache.avro.Schema.Parser().parse(is);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    BEAM_SCHEMA = Schema.builder()
            .addStringField("name")
            .addInt64Field("age")
            .build();
  }



  private static PCollection<Row> prepareData(Pipeline p, ParquetWordCount.WordCountOptions options) {
    //define the input row format
    Schema schema = AvroUtils.avroToBeamSchema(AVRO_SCHEMA);
    return p.apply(FileIO.match().filepattern(options.getInputFile()))
            .apply(FileIO.readMatches())
            .apply(ParquetIO.readFiles(AVRO_SCHEMA))
            .apply(MapElements.via(new SimpleFunction<GenericRecord, Row>() {
              @Override
              public Row apply(GenericRecord input) {
                String name = input.get("name").toString();
                long age = (long) input.get("age");
                return Row.withSchema(schema).addValues(name, age, null).build();
              }
            }))
            .setRowSchema(schema);

  }

  private static void queryData(PCollection<Row> inputTable) {
    //Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<Row> outputStream =
            inputTable.apply(SqlTransform.query("select name, age from PCOLLECTION where age >= 10"));

    // print the output record of case 1;
    outputStream.apply(
            "log_result",
            MapElements.via(
                    new SimpleFunction<Row, Void>() {
                      @Override
                      public @Nullable
                      Void apply(Row input) {
                        System.out.println("PCOLLECTION: " + input.getValues());
                        return null;
                      }
                    }));

    // Case 2. run the query with SqlTransform.query over result PCollection of case 1.
    PCollection<Row> outputStream2 =
            PCollectionTuple.of(new TupleTag<>("CASE1_RESULT"), outputStream)
                    .apply(SqlTransform.query("select name, sum(age) from CASE1_RESULT group by name"));

    // print the output record of case 2;
    outputStream2.apply(
            "log_result",
            MapElements.via(
                    new SimpleFunction<Row, Void>() {
                      @Override
                      public @Nullable
                      Void apply(Row input) {
                        // expect output:
                        //  CASE1_RESULT: [row, 5.0]
                        System.out.println("CASE1_RESULT: " + input.getValues());
                        return null;
                      }
                    }));
  }

  public static void main(String[] args) {
    ParquetWordCount.WordCountOptions options = PipelineOptionsFactory.fromArgs(args)
            .withValidation().as(ParquetWordCount.WordCountOptions.class);
    Pipeline p = Pipeline.create(options);
    PCollection<Row> inputTable = prepareData(p, options);

    queryData(inputTable);

    p.run().waitUntilFinish();
  }
}
