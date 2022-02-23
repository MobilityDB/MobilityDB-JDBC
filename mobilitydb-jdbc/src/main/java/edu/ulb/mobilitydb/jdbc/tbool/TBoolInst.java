package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;

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

}