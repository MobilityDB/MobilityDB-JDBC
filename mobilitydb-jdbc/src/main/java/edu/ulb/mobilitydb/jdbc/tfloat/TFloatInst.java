package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TFloatInst extends TInstant<Float> {
    public TFloatInst(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue);
    }

    public TFloatInst(float value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }

    @Override
    protected Temporal<Float> convert(Object obj) {
        if (obj instanceof TFloatInst) {
            return (TFloatInst) obj;
        }
        return null;
    }
}
