package nl.basjes.pig.stats.tdigest;

import com.tdunning.math.stats.TDigest;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;

public class Merge extends EvalFunc<Tuple> implements Algebraic {

  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return null;
    }

    return Utils.wrapTDigestIntoTuple(createDigest(input));
  }
  // ==========================================================================

  @Override
  public String getInitial() {
    return MergeTuplesIntoTDigest.class.getName();
  }

  @Override
  public String getIntermed() {
    return MergeTuplesIntoTDigest.class.getName();
  }

  @Override
  public String getFinal() {
    // The Final STILL returns a TDigest in a tuple !!
    // There are separate methods to extract the quantiles !!
    // This is more efficient because this was the digest only has to be calculated once.
    return MergeTuplesIntoTDigest.class.getName();
  }

  static public class MergeTuplesIntoTDigest extends EvalFunc<Tuple> {
    public Tuple exec(Tuple input) throws IOException {
      return Utils.wrapTDigestIntoTuple(createDigest(input));
    }
  }

  private static TDigest createDigest(Tuple input) throws ExecException {
    TDigest result = TDigest.createDigest(100.0);

    switch (input.getType(0)) {
      case DataType.BAG: {
        DataBag values = (DataBag) input.get(0);
        for (Tuple tuple : values) {
          result = mergeSingleTuple(result, tuple);
        }
      }
      break;
      default:
        result = mergeSingleTuple(result, input);
    }
    return result;
  }

  private static TDigest mergeSingleTuple(TDigest tDigest, Tuple tuple) throws ExecException {
    // Perhaps this is a tDigest tuple?
    TDigest tDigestFromTuple = Utils.unwrapTDigestFromTuple(tuple);
    if (tDigestFromTuple != null) {
      tDigest.add(tDigestFromTuple);
      return tDigest;
    }

    Object value = tuple.get(0);
    switch (tuple.getType(0)) {
      case DataType.INTEGER:
        tDigest.add((Integer) value);
        break;
      case DataType.LONG:
        tDigest.add((Long) value);
        break;
      case DataType.FLOAT:
        tDigest.add((Float) value);
        break;
      case DataType.DOUBLE:
        tDigest.add((Double) value);
        break;
      default:
        throw new ExecException("The datatype \"" + DataType.findTypeName(tuple.getType(0)) + "\"" +
                " (=" + tuple.getType(0) + ") cannot be merged into a tDigest.");
    }
    return tDigest;
  }

  @Override
  public Schema outputSchema(Schema input) {
    return Utils.getTDigestTupleSchema();
  }
}
