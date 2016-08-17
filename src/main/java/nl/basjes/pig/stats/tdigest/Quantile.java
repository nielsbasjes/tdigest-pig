package nl.basjes.pig.stats.tdigest;

import com.tdunning.math.stats.TDigest;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;

public class Quantile extends EvalFunc<Double> {

  // We expect a TDigest tuple and a DOUBLE or FLOAT (between 0 and 1)
  public Double exec(Tuple input) throws IOException {
    TDigest tDigest = Utils.unwrapTDigestFromTuple((Tuple) input.get(0));
    if (tDigest == null) {
      throw new ExecException("The first parameter was NOT a tDigest tuple.");
    }

    return tDigest.quantile((Double) input.get(1));
  }

  @Override
  public Schema getInputSchema() {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new FieldSchema("tDigest", Utils.getTDigestTupleSchema(), DataType.TUPLE));
      tupleSchema.add(new FieldSchema("quantile", DataType.DOUBLE));
      return tupleSchema;
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Schema outputSchema(Schema input) {
    // Validate input schema

    // Check that we were passed two fields
    if (input.size() != 2) {
      throw new RuntimeException(
              "Expected (tDigest, double), input does not have 2 fields");
    }

    // Check the types in the schema for both fields
    try {
      boolean field1Good = true;
      boolean field2Good = true;
      if (!Utils.isTDigestSchema(input.getField(0))) {
        field1Good = false;
      }

      if (input.getField(1).type != DataType.DOUBLE) {
        field2Good = false;
      }

      if (!(field1Good && field2Good)) {
        String msg = "Expected input (tDigest, int), received schema (";
        if (!field1Good) {
          msg += " BAD >>";
        }
        msg += DataType.findTypeName(input.getField(0).type);
        if (!field1Good) {
          msg += "<<";
        }
        msg += ", ";
        if (!field2Good) {
          msg += " BAD >>";
        }
        msg += DataType.findTypeName(input.getField(1).type);
        if (!field2Good) {
          msg += "<<";
        }
        msg += ")";
        throw new RuntimeException(msg);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Return output schema
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new FieldSchema("quantile", DataType.DOUBLE));
      return tupleSchema;
    } catch (Exception e) {
      return null;
    }

  }
}
