package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;

public class TFloatInstSet extends TInstantSet<Float, TFloat> {

    public TFloatInstSet(TFloat temporalDataType) throws Exception {
        super(temporalDataType);
    }

    public TFloatInstSet(String value) throws Exception {
        super(TFloat::new, value);
    }

    public TFloatInstSet(String[] values) throws Exception {
        super(TFloat::new, values);
    }

    public TFloatInstSet(TFloatInst[] values) throws Exception {
        super(TFloat::new, values);
    }

}
