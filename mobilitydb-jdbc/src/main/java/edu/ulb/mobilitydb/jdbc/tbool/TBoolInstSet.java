package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

public class TBoolInstSet extends TInstantSet<Boolean, TBool> {

    public TBoolInstSet(TBool temporalDataType) throws Exception {
        super(temporalDataType);
    }

    public TBoolInstSet(String value) throws Exception {
        super(TBool::new, value);
    }

    public TBoolInstSet(String[] values) throws Exception {
        super(TBool::new, values);
    }

    public TBoolInstSet(TBoolInst[] values) throws Exception {
        super(TBool::new, values);
    }

    @Override
    protected Temporal<Boolean, TBool> convert(Object obj) {
        if (obj instanceof TBoolInstSet) {
            return (TBoolInstSet) obj;
        }
        return null;
    }
}
