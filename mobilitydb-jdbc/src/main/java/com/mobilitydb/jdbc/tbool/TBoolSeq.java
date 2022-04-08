package com.mobilitydb.jdbc.tbool;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

public class TBoolSeq extends TSequence<Boolean> {
    public TBoolSeq(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue);
    }

    public TBoolSeq(String[] values) throws SQLException {
        super(values, TBool::getSingleTemporalValue);
    }

    public TBoolSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive, TBool::getSingleTemporalValue);
    }

    public TBoolSeq(TBoolInst[] values) throws SQLException {
        super(values);
    }

    public TBoolSeq(TBoolInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive);
    }
}
