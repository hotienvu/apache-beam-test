package beamtest.examples;

import beamtest.examples.model.Address;
import beamtest.examples.model.Person;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.example.model.Order;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import javax.annotation.Nullable;

class BeamSqlPojoExample {
  public static void main(String[] args) {
    Pipeline pipeline = createPipeline(args);

    // First step is to get PCollections of source objects.
    // In this example we create them directly in memory using Create.of().
    //
    // In real world such PCollections will likely be obtained from some other source,
    // e.g. a database or a text file. This process is not specific to Beam SQL,
    // please consult Beam programming guide for details.

    PCollection<Person> customers = loadCustomers(pipeline);
    PCollection<Order> orders = loadOrders(pipeline);

    // Example 1. Run a simple query over java objects:
    PCollection<Row> customersFromWonderland =
            customers.apply(
                    SqlTransform.query(
                            "SELECT id, name "
                                    + " FROM PCOLLECTION as P"
                                    + " WHERE P.address.street = 'Wonderland'"));

    // Output the results of the query:
    customersFromWonderland.apply(logRecords(": is from Wonderland"));

    // Example 2. Query the results of the first query:
    PCollection<Row> totalInWonderland =
            customersFromWonderland.apply(SqlTransform.query("SELECT COUNT(id) FROM PCOLLECTION"));

    // Output the results of the query:
    totalInWonderland.apply(logRecords(": total customers in Wonderland"));

    // Example 3. Query multiple PCollections of Java objects:
    PCollectionTuple cs = PCollectionTuple.of(new TupleTag<>("customers"), customers);
    PCollection<Row> ordersByAlice =
            PCollectionTuple.of(new TupleTag<>("customers"), customers)
                    .and(new TupleTag<>("orders"), orders)
                    .apply(
                            SqlTransform.query(
                                    "SELECT customers.name, ('order id:' || CAST(orders.id AS VARCHAR))"
                                            + " FROM orders "
                                            + "   JOIN customers ON orders.customerId = customers.id"
                                            + " WHERE customers.name = 'Alice'"));

    // Output the results of the query:
    ordersByAlice.apply(logRecords(": ordered by 'Alice'"));

    pipeline.run().waitUntilFinish();
  }

  private static MapElements<Row, Void> logRecords(String suffix) {
    return MapElements.via(
            new SimpleFunction<Row, Void>() {
              @Override
              public @Nullable Void apply(Row input) {
                System.out.println(input.getValues() + suffix);
                return null;
              }
            });
  }

  private static Pipeline createPipeline(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
    return Pipeline.create(options);
  }

  private static PCollection<Person> loadCustomers(Pipeline pipeline) {
    return pipeline.apply(
            Create.of(
                    new Person(1, 10, "Alice", new Address("Wonderland", 1)),
                    new Person(2, 20, "Bob", new Address("Mordor", 2)),
                    new Person(3, 30, "Charles", new Address("Wonderland", 3))
            ));
  }

  private static PCollection<Order> loadOrders(Pipeline pipeline) {
    return pipeline.apply(
            Create.of(
                    new Order(1, 3),
                    new Order(2, 2),
                    new Order(3, 1),
                    new Order(4, 3),
                    new Order(5, 1),
                    new Order(6, 2),
                    new Order(7, 2),
                    new Order(8, 3),
                    new Order(9, 1)));
  }
}