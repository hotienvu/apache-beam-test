package beamtest.examples.model;

import com.google.common.base.Objects;
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema;

import java.io.Serializable;

@DefaultSchema(JavaBeanSchema.class)
public class Person implements Serializable {

  private long id;
  private int age;
  private String name;
  private Address address;

  // must be declared for Beam to reflect
  public Person() {}

  public Person(long id, int age, String name, Address address) {
    this.id = id;
    this.age = age;
    this.name = name;
    this.address = address;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }


  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setAddress(Address address) {
    this.address = address;
  }

  public Address getAddress() {
    return address;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Person person = (Person) o;
    return id == person.id &&
            age == person.age &&
            Objects.equal(name, person.name) &&
            Objects.equal(address, person.address);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, age, name, address);
  }
}
