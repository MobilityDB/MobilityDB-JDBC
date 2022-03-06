package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TIntInst extends TInstant<Integer> {
    public TIntInst(String value) throws SQLException {
        super(value, TInt::getSingleTemporalValue);
    }

    public TIntInst(int value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }

    @Override
    protected Temporal<Integer> convert(Object obj) {
        if (obj instanceof TIntInst) {
            return (TIntInst) obj;
        }
        return null;
    }
}
