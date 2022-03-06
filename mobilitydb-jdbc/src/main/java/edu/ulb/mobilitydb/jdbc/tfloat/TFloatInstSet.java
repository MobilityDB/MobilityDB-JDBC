package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;

import java.sql.SQLException;

public class TFloatInstSet extends TInstantSet<Float> {
    public TFloatInstSet(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue);
    }

    public TFloatInstSet(String[] values) throws SQLException {
        super(values, TFloat::getSingleTemporalValue);
    }

    public TFloatInstSet(TFloatInst[] values) throws SQLException {
        super(values);
    }
}
