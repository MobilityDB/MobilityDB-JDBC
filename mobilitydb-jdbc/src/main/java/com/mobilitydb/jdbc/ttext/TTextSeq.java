package com.mobilitydb.jdbc.ttext;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

/**
 * By Default Interpolation is stepwise
 */
public class TTextSeq extends TSequence<String> {
    public TTextSeq(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
        stepwise = true;
    }

    public TTextSeq(String[] values) throws SQLException {
        super(true, values, TText::getSingleTemporalValue);
    }

    public TTextSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive, TText::getSingleTemporalValue);
    }

    public TTextSeq(TTextInst[] values) throws SQLException {
        super(true, values);
    }

    public TTextSeq(TTextInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(true, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected boolean explicitInterpolation() {
        return false;
    }
}
