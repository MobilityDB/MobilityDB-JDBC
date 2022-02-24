package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;
import java.time.OffsetDateTime;

public class TIntInst extends TInstant<Integer, TInt> {
    public TIntInst(TInt temporal) throws SQLException {
        super(temporal);
    }

    public TIntInst(String value) throws SQLException {
        super(TInt::new, value);
    }

    public TIntInst(int value, OffsetDateTime time) throws SQLException {
        super(TInt::new, value, time);
    }

    @Override
    protected Temporal<Integer, TInt> convert(Object obj) {
        if (obj instanceof TIntInst) {
            return (TIntInst) obj;
        }
        return null;
    }
}
