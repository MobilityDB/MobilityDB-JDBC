package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TTextInst extends TInstant<String, TText> {

    public TTextInst(TText temporal) throws SQLException {
        super(temporal);
    }

    public TTextInst(String value) throws SQLException {
        super(TText::new, value);
    }

    public TTextInst(String value, OffsetDateTime time) throws SQLException {
        super(TText::new, value, time);
    }

    @Override
    protected Temporal<String, TText> convert(Object obj) {
        if (obj instanceof TTextInst) {
            return (TTextInst) obj;
        }
        return null;
    }
}
