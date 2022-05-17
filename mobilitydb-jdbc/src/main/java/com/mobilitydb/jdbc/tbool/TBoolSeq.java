package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

/**
 * By Default Interpolation is stepwise
 */
public class TBoolSeq extends TSequence<Boolean> {
    public TBoolSeq(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue);
        isStepwise = true;
    }

    public TBoolSeq(String[] values) throws SQLException {
        super(true, values, TBool::getSingleTemporalValue);
    }

    public TBoolSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TBool::getSingleTemporalValue);
    }

    public TBoolSeq(TBoolInst[] values) throws SQLException {
        super(true, values);
    }

    public TBoolSeq(TBoolInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
