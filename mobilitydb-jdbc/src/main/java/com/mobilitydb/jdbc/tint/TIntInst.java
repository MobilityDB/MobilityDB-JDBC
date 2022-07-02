package com.mobilitydb.jdbc.tint;

import com.mobilitydb.jdbc.temporal.TInstant;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TIntInst extends TInstant<Integer> {
    public TIntInst(String value) throws SQLException {
        super(value, TInt::getSingleTemporalValue);
    }

    public TIntInst(int value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }
}
