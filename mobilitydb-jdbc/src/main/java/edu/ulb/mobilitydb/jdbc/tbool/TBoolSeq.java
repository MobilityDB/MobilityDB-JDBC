package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

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

    @Override
    protected Temporal<Boolean> convert(Object obj) {
        if (obj instanceof TBoolSeq) {
            return (TBoolSeq) obj;
        }
        return null;
    }
}
