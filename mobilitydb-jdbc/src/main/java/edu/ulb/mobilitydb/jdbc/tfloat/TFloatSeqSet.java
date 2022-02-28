package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;


import java.sql.SQLException;

public class TFloatSeqSet extends TSequenceSet<Float, TFloat> {
    public TFloatSeqSet(TFloat temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TFloatSeqSet(String value) throws SQLException {
        super(TFloat::new, value);
    }

    public TFloatSeqSet(String[] values) throws SQLException {
        super(TFloat::new, values);
    }

    public TFloatSeqSet(TFloatSeq[] values) throws SQLException {
        super(TFloat::new, values);
    }

    @Override
    protected Temporal<Float, TFloat> convert(Object obj) {
        if (obj instanceof TFloatSeqSet) {
            return (TFloatSeqSet) obj;
        }
        return null;
    }
}
