package com.mobilitydb.jdbc.tfloat;

import com.mobilitydb.jdbc.temporal.TSequence;

import java.sql.SQLException;

public class TFloatSeq extends TSequence<Float> {
    public TFloatSeq(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(String[] values) throws SQLException {
        super(false, values, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(boolean isStepwise, String[] values) throws SQLException {
        super(isStepwise, values, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(false, values, lowerInclusive, upperInclusive, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(boolean isStepwise, String[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive, TFloat::getSingleTemporalValue);
    }

    public TFloatSeq(TFloatInst[] values) throws SQLException {
        super(false, values);
    }

    public TFloatSeq(boolean isStepwise, TFloatInst[] values) throws SQLException {
        super(isStepwise, values);
    }

    public TFloatSeq(TFloatInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(false, values, lowerInclusive, upperInclusive);
    }

    public TFloatSeq(boolean isStepwise, TFloatInst[] values, boolean lowerInclusive, boolean upperInclusive)
            throws SQLException {
        super(isStepwise, values, lowerInclusive, upperInclusive);
    }

}
