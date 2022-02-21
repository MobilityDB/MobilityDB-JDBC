package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TemporalInstant;
import edu.ulb.mobilitydb.jdbc.time.Period;

import java.util.stream.Collectors;

public class TIntInst extends TemporalInstant<Integer,TInt> {

    public TIntInst(TInt temporal) throws Exception {
        super(temporal);
    }

}
