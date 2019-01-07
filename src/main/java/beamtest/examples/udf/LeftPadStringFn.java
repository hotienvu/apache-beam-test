package beamtest.examples.udf;

import org.apache.beam.sdk.extensions.sql.BeamSqlUdf;
import org.apache.commons.lang.StringUtils;

public class LeftPadStringFn implements BeamSqlUdf {
  public static String eval(String str, Integer len, String padStr) {
    return StringUtils.leftPad(str, len, padStr);
  }
}
