package com.mobilitydb.jdbc.tfloat;

import com.mobilitydb.jdbc.temporal.TInstant;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TFloatInst extends TInstant<Float> {
    public TFloatInst(String value) throws SQLException {
        super(value, TFloat::getSingleTemporalValue);
    }

    public TFloatInst(float value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }
}