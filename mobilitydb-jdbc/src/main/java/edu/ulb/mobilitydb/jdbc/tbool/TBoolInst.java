package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TemporalInstant;

public class TBoolInst extends TemporalInstant<Boolean, TBool> {

    public TBoolInst(TBool temporal) throws Exception {
        super(temporal);
    }

}