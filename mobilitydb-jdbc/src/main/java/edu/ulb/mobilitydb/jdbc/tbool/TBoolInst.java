package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.time.OffsetDateTime;

public class TBoolInst extends TInstant<Boolean, TBool> {

    public TBoolInst(TBool temporal) throws Exception {
        super(temporal);
    }

    public TBoolInst(String value) throws Exception {
        super(TBool::new, value);
    }

    public TBoolInst(boolean value, OffsetDateTime time) throws Exception {
        super(TBool::new, value, time);
    }

    @Override
    protected Temporal<Boolean, TBool> convert(Object obj) {
        if (obj instanceof TBoolInst) {
            return (TBoolInst) obj;
        }
        return null;
    }
}