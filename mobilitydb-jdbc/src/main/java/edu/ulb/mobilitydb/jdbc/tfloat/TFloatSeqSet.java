package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;


import java.sql.SQLException;

public class TFloatSeqSet extends TSequenceSet<Float> {
    public TFloatSeqSet(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue);
    }

    public TFloatSeqSet(String[] values) throws SQLException {
        super(values, TFloat::getSingleTemporalValue);
    }

    public TFloatSeqSet(TFloatSeq[] values) throws SQLException {
        super(values, TFloat::getSingleTemporalValue);
    }

    @Override
    protected Temporal<Float> convert(Object obj) {
        if (obj instanceof TFloatSeqSet) {
            return (TFloatSeqSet) obj;
        }
        return null;
    }
}
