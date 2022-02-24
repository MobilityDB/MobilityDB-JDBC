package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TFloatInstSet extends TInstantSet<Float, TFloat> {

    public TFloatInstSet(TFloat temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TFloatInstSet(String value) throws SQLException {
        super(TFloat::new, value);
    }

    public TFloatInstSet(String[] values) throws SQLException {
        super(TFloat::new, values);
    }

    public TFloatInstSet(TFloatInst[] values) throws SQLException {
        super(TFloat::new, values);
    }

    @Override
    protected Temporal<Float, TFloat> convert(Object obj) {
        if (obj instanceof TFloatInstSet) {
            return (TFloatInstSet) obj;
        }
        return null;
    }
}
