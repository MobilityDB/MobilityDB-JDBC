package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;

import java.time.OffsetDateTime;

public class TIntInst extends TInstant<Integer, TInt> {

    public TIntInst(TInt temporal) throws Exception {
        super(temporal);
    }

    public TIntInst(String value) throws Exception {
        super(TInt::new, value);
    }

    public TIntInst(int value, OffsetDateTime time) throws Exception {
        super(TInt::new, value, time);
    }

}
