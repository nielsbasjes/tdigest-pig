package nl.basjes.pig.stats.tdigest;

import java.io.IOException;

import com.tdunning.math.stats.TDigest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.Algebraic;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class Merge extends EvalFunc<Tuple> implements Algebraic {
  private static final Log LOG = LogFactory.getLog(Merge.class);

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
    
    LOG.error("MERGING THIS TUPLE" + tuple.toDelimitedString(";"));

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
        TDigest tDigestFromTuple = Utils.unwrapTDigestFromTuple((Tuple) value);
        if (tDigestFromTuple != null) {
          tDigest.add(tDigestFromTuple);
          break;
        }
        if (Utils.isTDigestTuple((Tuple) value)){
          throw new ExecException("The tDigest tuple could not be unwrapped.");
        }
        // Switch fallthrough if not a tDigest !!
      default:
        // Perhaps the entire thing was a tDigest tuple?
        tDigestFromTuple = Utils.unwrapTDigestFromTuple(tuple);
        if (tDigestFromTuple != null) {
          tDigest.add(tDigestFromTuple);
          return tDigest;
        }

        throw new ExecException("The datatype " + tuple.getType(0) +
                "(="+ DataType.findTypeName(tuple.getType(0))+") cannot be merged into a tDigest.");
    }
    return tDigest;
  }

  @Override
  public Schema outputSchema(Schema input) {
    return Utils.getTDigestTupleSchema();
  }
}
