package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TIntSeqSet extends TSequenceSet<Integer> {
    public TIntSeqSet(String value) throws SQLException {
        super(value, TInt::getSingleTemporalValue);
        stepwise = true;
    }

    public TIntSeqSet(String[] values) throws SQLException {
        super(true, values, TInt::getSingleTemporalValue);
    }

    public TIntSeqSet(TIntSeq[] values) throws SQLException {
        super(true, values, TInt::getSingleTemporalValue);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
