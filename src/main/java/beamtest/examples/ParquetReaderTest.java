package beamtest.examples;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ParquetReaderTest {
    private static Schema SCHEMA;

    static {
        try {
            SCHEMA = new Schema.Parser().parse(new File("people.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static void writeParquet() throws IOException {
        for (Schema.Field field: SCHEMA.getFields()) {
            System.out.println(field);
        }
        GenericRecord alice = new GenericRecordBuilder(SCHEMA)
                .set("name", "Alice")
                .set("age", 10)
                .build();

        GenericRecord bob = new GenericRecordBuilder(SCHEMA)
                .set("name", "Bob")
                .set("age", 20)
                .build();

        List<GenericRecord> recordsToWrite = new ArrayList<>(Arrays.asList(alice, bob, alice));
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter
                .<GenericRecord>builder(new Path("people.parquet"))
                .withSchema(SCHEMA)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {
            for (GenericRecord record : recordsToWrite) {
                writer.write(record);
            }
        }
    }


    private static void readParquet() throws IOException {
        ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(new Path("people.parquet")).build();
        GenericRecord record;
        while ((record = reader.read())  != null) {
            System.out.println(record.get("name"));
        }
    }

    public static void main(String[] args) {
        try {
            writeParquet();
            readParquet();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
