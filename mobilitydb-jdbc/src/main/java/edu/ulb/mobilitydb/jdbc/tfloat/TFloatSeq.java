package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

public class TFloatSeq extends TSequence<Float> {
    public TFloatSeq(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(String[] values) throws SQLException {
        super(values, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(TFloatInst[] values) throws SQLException {
        super(values);
    }

    public TFloatSeq(TFloatInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(values, lowerInclusive, upperInclusive);
    }
}
