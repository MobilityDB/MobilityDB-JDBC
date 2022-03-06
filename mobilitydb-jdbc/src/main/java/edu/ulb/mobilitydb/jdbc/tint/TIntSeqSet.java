package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TIntSeqSet extends TSequenceSet<Integer> {
    public TIntSeqSet(String value) throws SQLException {
        super(value, TInt::getSingleTemporalValue);
    }

    public TIntSeqSet(String[] values) throws SQLException {
        super(values, TInt::getSingleTemporalValue);
    }

    public TIntSeqSet(TIntSeq[] values) throws SQLException {
        super(values, TInt::getSingleTemporalValue);
    }

    @Override
    protected Temporal<Integer> convert(Object obj) {
        if (obj instanceof TIntSeqSet) {
            return (TIntSeqSet) obj;
        }
        return null;
    }
}
