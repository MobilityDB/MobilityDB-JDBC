package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstant;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;
import edu.ulb.mobilitydb.jdbc.tint.TIntInst;

import java.time.OffsetDateTime;

public class TFloatInst extends TInstant<Float, TFloat> {

    public TFloatInst(TFloat temporal) throws Exception {
        super(temporal);
    }

    public TFloatInst(String value) throws Exception {
        super(TFloat::new, value);
    }

    public TFloatInst(float value, OffsetDateTime time) throws Exception {
        super(TFloat::new, value, time);
    }

    @Override
    protected Temporal<Float, TFloat> convert(Object obj) {
        if (obj instanceof TFloatInst) {
            return (TFloatInst) obj;
        }
        return null;
    }
}
