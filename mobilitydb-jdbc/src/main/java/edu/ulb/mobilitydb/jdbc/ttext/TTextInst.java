package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TTextInst extends TInstant<String> {
    public TTextInst(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
    }

    public TTextInst(String value, OffsetDateTime time) throws SQLException {
        super(value, time);
    }

    @Override
    protected Temporal<String> convert(Object obj) {
        if (obj instanceof TTextInst) {
            return (TTextInst) obj;
        }
        return null;
    }
}
