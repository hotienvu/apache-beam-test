package beamtest.examples;

import beamtest.examples.common.KafkaIOs;
import beamtest.examples.common.KafkaOptions;
import beamtest.examples.model.Person;
import com.alibaba.fastjson.JSON;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.example.model.Order;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;

public class StreamingSQL {

  public static void main(String[] args) {
    KafkaOptions options = PipelineOptionsFactory
      .fromArgs(args).withValidation().as(KafkaOptions.class);

    Pipeline pipeline = Pipeline.create(options);

    PCollection<Person> customer = pipeline
      .apply(KafkaIOs.readFromKafka("customer", "localhost:9092"))
      .apply(MapElements.into(TypeDescriptor.of(Person.class))
        .via(it -> JSON.parseObject(it.getKV().getValue(), Person.class)))
      .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))));

    PCollection<Order> orders = pipeline
      .apply(KafkaIOs.readFromKafka("order", "localhost:9092"))
      .apply(MapElements.into(TypeDescriptor.of(Order.class))
        .via(it -> JSON.parseObject(it.getKV().getValue(), Order.class)))
      .apply(Window.into(FixedWindows.of(Duration.standardSeconds(10))));

//
    PCollectionTuple.of(new TupleTag<>("customers"), customer)
      .and(new TupleTag<>("orders"), orders)
      .apply(SqlTransform.query("SELECT customers.name, ('order id:' || CAST(orders.id AS VARCHAR))"
        + " FROM customers "
        + "   JOIN orders ON orders.customerId = customers.id"
        + " WHERE customers.name = 'Alice'"))
      .apply(MapElements.into(TypeDescriptors.strings())
        .via(Row::toString))
      .apply(KafkaIOs.writeToKafka("test-output", "localhost:9092"));

    pipeline.run().waitUntilFinish();
  }
}
