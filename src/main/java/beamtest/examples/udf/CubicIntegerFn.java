package beamtest.examples.udf;

import org.apache.beam.sdk.transforms.SerializableFunction;

public class CubicIntegerFn implements SerializableFunction<Long, Long> {
  @Override
  public Long apply(Long input) {
    return input * input * input;
  }
}
