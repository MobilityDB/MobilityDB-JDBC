package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TemporalInstant;

import java.time.OffsetDateTime;
import java.util.function.Supplier;

public class TFloatInst extends TemporalInstant<Float, TFloat> {

    public TFloatInst(TFloat temporal) throws Exception {
        super(temporal);
    }

    public TFloatInst(String value) throws Exception {
        super(TFloat::new, value);
    }

    public TFloatInst(float value, OffsetDateTime time) throws Exception {
        super(TFloat::new, value, time);
    }
}
