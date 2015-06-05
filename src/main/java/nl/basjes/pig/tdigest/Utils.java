package nl.basjes.pig.tdigest;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.nio.ByteBuffer;

public class Utils {
  // ==========================================================================

  // The TDigest 'tuple' consists of two fields:
  // - 0: chararray with the magic value 't-Digest'
  // - 1: bytearray which is the serialized TDigest

  // The purpose of the first one is ONLY to ensure we got a 'TDigest Tuple'

  private static final String TDIGEST_TUPLE_MARKER = "t-Digest";

  static Tuple wrapTDigestIntoTuple(TDigest tDigest) throws ExecException {
    if (tDigest == null) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.allocate(100000); // 100K bytes initially
    tDigest.asSmallBytes(buffer);
    DataByteArray byteArray = new DataByteArray(buffer.array());

    Tuple tDigestTuple = TupleFactory.getInstance().newTuple(2); // 2 fields
    tDigestTuple.set(0, TDIGEST_TUPLE_MARKER);
    tDigestTuple.set(1, byteArray);
    return tDigestTuple;
  }


  /**
   * Determine if this is a TDigest tuple
   * @param tuple
   * @return true if this tuple looks like a TDigest tuple
   */
  static boolean isTDigestTuple(Tuple tuple) throws ExecException {
    if (tuple == null ||
            tuple.getType(0) != DataType.CHARARRAY ||
            TDIGEST_TUPLE_MARKER.equals(tuple.get(0)) ||
            tuple.getType(1) != DataType.BYTEARRAY
            ) {
      return false;
    }
    return true;
  }

  /**
   * Extracts the TDigest object from the tuple (iff present)
   * @param tuple
   * @return TDigest block OR null if this is NOT a TDigest at all
   */
  static TDigest unwrapTDigestFromTuple(Tuple tuple) throws ExecException {
    if (!isTDigestTuple(tuple)) {
      return null;
    }

    DataByteArray theData = (DataByteArray)tuple.get(1);
    ByteBuffer byteBuffer = ByteBuffer.wrap(theData.get());
    return AVLTreeDigest.fromBytes(byteBuffer);
  }

}
