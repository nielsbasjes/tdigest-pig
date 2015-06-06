package nl.basjes.pig.stats.tdigest;

import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.util.List;

import static org.apache.pig.builtin.mock.Storage.resetData;
import static org.apache.pig.builtin.mock.Storage.tuple;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestTDigestUDF {

  @Test
  public void testTDigestUDF() throws Exception {
    PigServer pigServer = new PigServer(ExecType.LOCAL);
    Storage.Data data = resetData(pigServer);

    data.set("input", "value:long",
            tuple(1),
            tuple(2L),
            tuple(3.0F),
            tuple(4.0D),
            tuple(5)
    );

    pigServer.registerQuery(
            "InputData =" +
                    "    LOAD 'input'" +
                    "    USING mock.Storage();");

    pigServer.registerQuery(
            "GroupedData =" +
                    "    GROUP    InputData ALL;");

    pigServer.registerQuery(
            "TDGroup =" +
                    "    FOREACH  GroupedData" +
                    "    GENERATE nl.basjes.pig.stats.tdigest.Merge(InputData.value) AS tDigest;");

    pigServer.registerQuery(
            "TDStats =" +
                    "    FOREACH  TDGroup" +
                    "    GENERATE nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.90) AS Precentile90:double," +
                    "             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.75) AS Precentile75:double," +
                    "             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.50) AS Precentile50:double," +
                    "             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.25) AS Precentile25:double," +
                    "             nl.basjes.pig.stats.tdigest.Quantile(tDigest,0.10) AS Precentile10:double;");

    pigServer.registerQuery(
            "STORE TDStats " +
                    "    INTO 'ResultSet' " +
                    "    USING mock.Storage();");

    List<Tuple> out = data.get("ResultSet");

    // Check the result
    assertTrue("Too many records", out.size() == 1);

    byte fieldType;
    Tuple tuple = out.get(0);

    fieldType = ((Tuple) tuple.get(0)).getType(0);
    assertEquals("Bad data type for Q90: " + DataType.findTypeName(fieldType), fieldType, DataType.DOUBLE);

    fieldType = ((Tuple) tuple.get(1)).getType(0);
    assertEquals("Bad data type for Q75: " + DataType.findTypeName(fieldType), fieldType, DataType.DOUBLE);

    fieldType = ((Tuple) tuple.get(2)).getType(0);
    assertEquals("Bad data type for Q50: " + DataType.findTypeName(fieldType), fieldType, DataType.DOUBLE);

    fieldType = ((Tuple) tuple.get(3)).getType(0);
    assertEquals("Bad data type for Q25: " + DataType.findTypeName(fieldType), fieldType, DataType.DOUBLE);

    fieldType = ((Tuple) tuple.get(4)).getType(0);
    assertEquals("Bad data type for Q10: " + DataType.findTypeName(fieldType), fieldType, DataType.DOUBLE);

    assertEquals("Bad data Q90", (Double) ((Tuple) tuple.get(0)).get(0), 4.60D, 0.1D);
    assertEquals("Bad data Q75", (Double) ((Tuple) tuple.get(1)).get(0), 4.00D, 0.1D);
    assertEquals("Bad data Q50", (Double) ((Tuple) tuple.get(2)).get(0), 3.00D, 0.1D);
    assertEquals("Bad data Q25", (Double) ((Tuple) tuple.get(3)).get(0), 2.00D, 0.1D);
    assertEquals("Bad data Q10", (Double) ((Tuple) tuple.get(4)).get(0), 1.40D, 0.1D);
  }

}
