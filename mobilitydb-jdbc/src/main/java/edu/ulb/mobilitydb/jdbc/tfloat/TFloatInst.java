package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TFloatInst extends TInstant<Float, TFloat> {

    public TFloatInst(TFloat temporal) throws SQLException {
        super(temporal);
    }

    public TFloatInst(String value) throws SQLException {
        super(TFloat::new, value);
    }

    public TFloatInst(float value, OffsetDateTime time) throws SQLException {
        super(TFloat::new, value, time);
    }

    @Override
    protected Temporal<Float, TFloat> convert(Object obj) {
        if (obj instanceof TFloatInst) {
            return (TFloatInst) obj;
        }
        return null;
    }
}
