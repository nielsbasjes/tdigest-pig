package nl.basjes.pig.tdigest;

import java.io.IOException;

import com.tdunning.math.stats.TDigest;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import static nl.basjes.pig.tdigest.Utils.unwrapTDigestFromTuple;
import static nl.basjes.pig.tdigest.Utils.wrapTDigestIntoTuple;

public class Merge extends EvalFunc<Tuple> implements Algebraic {

  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() == 0) {
      return null;
    }

    return wrapTDigestIntoTuple(createDigest(input));
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
    return MergeTuplesIntoTDigest.class.getName();
  }

  static public class MergeTuplesIntoTDigest extends EvalFunc<Tuple> {
    public Tuple exec(Tuple input) throws IOException {
      return wrapTDigestIntoTuple(createDigest(input));
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
      }; break;
      default:
        result = mergeSingleTuple(result, input);
    }
    return result;
  }

  private static TDigest mergeSingleTuple(TDigest tDigest, Tuple tuple) throws ExecException {
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
      case DataType.TUPLE:
        TDigest tDigestFromTuple = unwrapTDigestFromTuple((Tuple) value);
        if (tDigestFromTuple != null) {
          tDigest.add(tDigestFromTuple);
          break;
        }
        // Switch fallthrough if not a tDigest !!
      default:
        throw new ExecException("The datatype " + tuple.getType(0) +
                "(="+ DataType.findTypeName(tuple.getType(0))+") cannot be merged into a tDigest.");
    }
    return tDigest;
  }

  @Override
  public Schema outputSchema(Schema input) {
    try {
      Schema tupleSchema = new Schema();
//      tupleSchema.add(new Schema.FieldSchema("magicValue", DataType.CHARARRAY));
      tupleSchema.add(new Schema.FieldSchema("tDigest", DataType.TUPLE));
      return tupleSchema;
    } catch (Exception e) {
      return null;
    }
  }
}
