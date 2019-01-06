package beamtest.examples.model;

import com.google.common.base.Objects;
import org.apache.beam.sdk.schemas.DefaultSchema;
import org.apache.beam.sdk.schemas.JavaBeanSchema;

import java.io.Serializable;

@DefaultSchema(JavaBeanSchema.class)
public class Address implements Serializable {
  private String street;
  private int postal;

  // must be declared for Beam to reflect
  public Address() {}

  public Address(String street, int postal) {
    this.street = street;
    this.postal = postal;
  }

  public String getStreet() {
    return street;
  }

  public void setStreet(String street) {
    this.street = street;
  }

  public int getPostal() {
    return postal;
  }

  public void setPostal(int postal) {
    this.postal = postal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Address address = (Address) o;
    return postal == address.postal &&
            Objects.equal(street, address.street);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(street, postal);
  }
}
