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
  // - 0: long with the magic value
  // - 1: bytearray which is the serialized TDigest

  // The purpose of the first one is ONLY to ensure we got a 'TDigest Tuple'

  private static final Long TDIGEST_TUPLE_MARKER = 11668103115116L; // = ASCII Bytes 'tDgst' as Long

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
   * @param tuple Is this really a tuple that contains a TDigest?
   * @return true if this tuple looks like a TDigest tuple
   */
  static boolean isTDigestTuple(Tuple tuple) throws ExecException {
    return (
      tuple != null &&
      tuple.size() == 2 &&
      TDIGEST_TUPLE_MARKER.equals(tuple.get(0)) &&
      tuple.getType(0) == DataType.LONG &&
      tuple.getType(1) == DataType.BYTEARRAY
    );
  }

  /**
   * Extracts the TDigest object from the tuple (iff present)
   *
   * @param tuple The tuple from which the TDigest datastructure needs to be extracted
   * @return TDigest block OR null if this is NOT a TDigest at all
   */
  static TDigest unwrapTDigestFromTuple(Tuple tuple) throws ExecException {
    if (!isTDigestTuple(tuple)) {
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
      Schema tupleSchema = new Schema();
      tupleSchema.add(new Schema.FieldSchema("tDigestTuple", getTDigestSchema(), DataType.TUPLE));
      return tupleSchema;
    } catch (Exception e) {
      return null;
    }
  }

  static public Schema getTDigestSchema() {
    try {
      Schema tDigestSchema = new Schema();
      tDigestSchema.add(new Schema.FieldSchema("magicValue", DataType.LONG));
      tDigestSchema.add(new Schema.FieldSchema("tDigest", DataType.BYTEARRAY));
      return tDigestSchema;
    } catch (Exception e) {
      return null;
    }
  }

  static public boolean isTDigestSchema(Schema.FieldSchema fieldSchema) {
    try {
      if (fieldSchema.type != DataType.TUPLE) {
        return false;
      }
      Schema schema = fieldSchema.schema;
      return (
            schema.size() == 2 &&
            schema.getField(0).type == DataType.LONG &&
            schema.getField(1).type == DataType.BYTEARRAY);
    } catch (Exception e) {
      return false;
    }
  }


}
