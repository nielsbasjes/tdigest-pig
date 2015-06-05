package nl.basjes.pig.tdigest;

import java.io.IOException;

import com.tdunning.math.stats.TDigest;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.builtin.mock.Storage;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TDigestQuantile extends EvalFunc<Tuple> {

  // We expect a TDigest tuple and a DOUBLE or FLOAT (between 0 and 1)
  public Tuple exec(Tuple input) throws IOException {
    if (input == null ||
        input.size() != 2 ||
        input.getType(0) != DataType.TUPLE ||
        ( input.getType(1) != DataType.FLOAT  &&
          input.getType(1) != DataType.DOUBLE )
       ) {
      throw new ExecException(this.getClass().getCanonicalName() +
              " needs two parameters: <TDigest Tuple> <Double>.");
    }

    Tuple tDigestTuple = (Tuple) input.get(0);

    TDigest tDigest = Utils.unwrapTDigestFromTuple(tDigestTuple);
    if (tDigest == null) {
      throw new ExecException("The first parameter was NOT a tDigest tuple");
    }

    Double quantile;
    switch (         input.getType(1)) {
      case DataType.FLOAT:
        quantile = new Double((Float)input.get(1));
        break;
      case DataType.DOUBLE:
        quantile = (Double)input.get(1);
        break;
      default:
        throw new ExecException(this.getClass().getCanonicalName() +
                " this shouldn't occur, we already checked this case");
    }

    Tuple output = TupleFactory.getInstance().newTuple(1);
    output.set(0, tDigest.quantile(quantile));

    return output;
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
      tupleSchema.add(new FieldSchema("quantile", DataType.DOUBLE));
      return tupleSchema;
    } catch (Exception e) {
      return null;
    }
  }

}
