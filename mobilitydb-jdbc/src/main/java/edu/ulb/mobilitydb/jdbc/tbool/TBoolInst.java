package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TBoolInst extends TInstant<Boolean> {
    public TBoolInst(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue);
    }

    public TBoolInst(boolean value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }

    @Override
    protected Temporal<Boolean> convert(Object obj) {
        if (obj instanceof TBoolInst) {
            return (TBoolInst) obj;
        }
        return null;
    }
}