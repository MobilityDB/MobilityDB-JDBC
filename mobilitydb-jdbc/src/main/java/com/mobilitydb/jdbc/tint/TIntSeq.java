package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

public class TIntSeq extends TSequence<Integer> {
    public TIntSeq(String value) throws SQLException {
        super(value, TInt::getSingleTemporalValue);
    }

    public TIntSeq(String[] values) throws SQLException {
        super(values, TInt::getSingleTemporalValue);
    }

    public TIntSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive, TInt::getSingleTemporalValue);
    }

    public TIntSeq(TIntInst[] values) throws SQLException {
        super(values);
    }

    public TIntSeq(TIntInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive);
    }
}
