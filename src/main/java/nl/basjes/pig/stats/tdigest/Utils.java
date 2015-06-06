package nl.basjes.pig.stats.tdigest;

import com.tdunning.math.stats.AVLTreeDigest;
import com.tdunning.math.stats.TDigest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.nio.ByteBuffer;

public class Utils {
  // ==========================================================================
  private static final Log LOG = LogFactory.getLog(Utils.class);

  // The TDigest 'tuple' consists of two fields:
  // - 0: chararray with the magic value 't-Digest'
  // - 1: bytearray which is the serialized TDigest

  // The purpose of the first one is ONLY to ensure we got a 'TDigest Tuple'

  private static final String TDIGEST_TUPLE_MARKER = "@@ t-Digest @@ MAGIC @@";

  static Tuple wrapTDigestIntoTuple(TDigest tDigest) throws ExecException {
    if (tDigest == null) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.allocate(tDigest.smallByteSize());
    tDigest.asSmallBytes(buffer);
    DataByteArray byteArray = new DataByteArray(buffer.array());

    Tuple tDigestTuple = TupleFactory.getInstance().newTuple(2); // 2 fields
    tDigestTuple.set(0, TDIGEST_TUPLE_MARKER);
    tDigestTuple.set(1, byteArray);
    return tDigestTuple;
  }


  /**
   * Determine if this is a TDigest tuple
   *
   * @param tuple
   * @return true if this tuple looks like a TDigest tuple
   */
  static boolean isTDigestTuple(Tuple tuple) throws ExecException {


    if (tuple == null) {
      LOG.error("Tuple is NOT tDigest: is NULL");
      return false;
    }
    if (tuple.getType(0) != DataType.CHARARRAY) {
      LOG.error("Tuple is NOT tDigest: First field is not chararray");
      return false;
    }
    if (!TDIGEST_TUPLE_MARKER.equals(tuple.get(0))) {
      LOG.error("Tuple is NOT tDigest: First field is not magic value");
      return false;
    }
    if (tuple.getType(1) != DataType.BYTEARRAY) {
      LOG.error("Tuple is NOT tDigest: Second field is not BYTEARRAY");
      return false;
    }

    return true;
//    return (
//            tuple != null &&
//            tuple.getType(0) == DataType.CHARARRAY &&
//            TDIGEST_TUPLE_MARKER.equals(tuple.get(0)) &&
//            tuple.getType(1) == DataType.BYTEARRAY
//            );
  }

  /**
   * Extracts the TDigest object from the tuple (iff present)
   *
   * @param tuple
   * @return TDigest block OR null if this is NOT a TDigest at all
   */
  static TDigest unwrapTDigestFromTuple(Tuple tuple) throws ExecException {
    if (!isTDigestTuple(tuple)) {
      LOG.error("CANNOT UNWRAP");

      return null;
    }

    DataByteArray theData = (DataByteArray) tuple.get(1);
    ByteBuffer byteBuffer = ByteBuffer.wrap(theData.get());
    TDigest tDigest = AVLTreeDigest.fromBytes(byteBuffer);
    if (tDigest == null) {
      LOG.error("Tried to deserialize a tDigest bytes array and FAILED");
    }
    return tDigest;
  }


  static public Schema getTDigestTupleSchema() {
    try {
      Schema tDigestSchema = new Schema();
      tDigestSchema.add(new Schema.FieldSchema("magicValue", DataType.CHARARRAY));
      tDigestSchema.add(new Schema.FieldSchema("tDigest", DataType.BYTEARRAY));

      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("tDigestTuple", tDigestSchema, DataType.TUPLE));
      LOG.error("SOMEONE WANTED THE SCHEMA");
      return tupleSchema;
    } catch (Exception e) {
      return null;
    }
  }

}
