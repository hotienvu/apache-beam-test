package beamtest.examples.common;


import io.vavr.control.Either;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AvroUtils {
  private static final Set<Schema.Type> PRIMITIVES = new HashSet<>();
  private static final Map<Type, org.apache.beam.sdk.schemas.Schema.FieldType> AVRO_BEAM_TYPES = new HashMap<>();

  static {
    PRIMITIVES.add(Type.STRING);
    PRIMITIVES.add(Type.BYTES);
    PRIMITIVES.add(Type.INT);
    PRIMITIVES.add(Type.LONG);
    PRIMITIVES.add(Type.FLOAT);
    PRIMITIVES.add(Type.DOUBLE);
    PRIMITIVES.add(Type.BOOLEAN);


    AVRO_BEAM_TYPES.put(Type.STRING, org.apache.beam.sdk.schemas.Schema.FieldType.STRING);
    AVRO_BEAM_TYPES.put(Type.BYTES, org.apache.beam.sdk.schemas.Schema.FieldType.BYTES);
    AVRO_BEAM_TYPES.put(Type.INT, org.apache.beam.sdk.schemas.Schema.FieldType.INT32);
    AVRO_BEAM_TYPES.put(Type.LONG, org.apache.beam.sdk.schemas.Schema.FieldType.INT64);
    AVRO_BEAM_TYPES.put(Type.FLOAT, org.apache.beam.sdk.schemas.Schema.FieldType.FLOAT);
    AVRO_BEAM_TYPES.put(Type.DOUBLE, org.apache.beam.sdk.schemas.Schema.FieldType.DOUBLE);
    AVRO_BEAM_TYPES.put(Type.BOOLEAN, org.apache.beam.sdk.schemas.Schema.FieldType.BOOLEAN);


  }
  public static org.apache.beam.sdk.schemas.Schema avroToBeamSchema(Schema avroSchema) {
    // the result is guaranteed to be a Beam Schema
    return traverseAndAdd("", avroSchema).get();
  }

  private static Either<org.apache.beam.sdk.schemas.Schema.Field, org.apache.beam.sdk.schemas.Schema> traverseAndAdd(String fieldName, Schema avroSchema) {
    Type type = avroSchema.getType();
    if (PRIMITIVES.contains(type)) {
      return Either.left(org.apache.beam.sdk.schemas.Schema.Field.of(fieldName, AVRO_BEAM_TYPES.get(type)));
    }
    org.apache.beam.sdk.schemas.Schema.Builder builder = org.apache.beam.sdk.schemas.Schema.builder();
    for (Schema.Field field: avroSchema.getFields()) {
      Either<org.apache.beam.sdk.schemas.Schema.Field, org.apache.beam.sdk.schemas.Schema> beamField = traverseAndAdd(field.name(), field.schema());
      if (beamField.isLeft()) {
        builder.addField(beamField.getLeft());
      } else {
        builder.addNullableField(field.name(), org.apache.beam.sdk.schemas.Schema.FieldType.row(beamField.right().get()));
      }
    }
    return Either.right(builder.build());
  }
}
