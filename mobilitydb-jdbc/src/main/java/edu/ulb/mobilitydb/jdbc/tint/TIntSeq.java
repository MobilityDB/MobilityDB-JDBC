package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

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

    @Override
    protected Temporal<Integer> convert(Object obj) {
        if (obj instanceof TIntSeq) {
            return (TIntSeq) obj;
        }
        return null;
    }
}
