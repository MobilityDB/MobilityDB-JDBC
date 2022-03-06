package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

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

    @Override
    protected Temporal<Float> convert(Object obj) {
        if (obj instanceof TFloatInstSet) {
            return (TFloatInstSet) obj;
        }
        return null;
    }
}
