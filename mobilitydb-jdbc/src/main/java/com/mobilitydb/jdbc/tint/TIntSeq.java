package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

/**
 * By Default Interpolation is stepwise
 */
public class TIntSeq extends TSequence<Integer> {
    public TIntSeq(String value) throws SQLException {
        super(value, TInt::getSingleTemporalValue);
        stepwise = true;
    }

    public TIntSeq(String[] values) throws SQLException {
        super(true, values, TInt::getSingleTemporalValue);
    }

    public TIntSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TInt::getSingleTemporalValue);
    }

    public TIntSeq(TIntInst[] values) throws SQLException {
        super(true, values);
    }

    public TIntSeq(TIntInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
