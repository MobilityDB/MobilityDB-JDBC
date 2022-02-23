package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;

public class TIntInstSet extends TInstantSet<Integer, TInt> {

    //READ
    public TIntInstSet(TInt temporalDataType) throws Exception {
        super(temporalDataType);
    }

    //WRITE
    //String of "{10@2019-09-08, 20@2019-09-09, 20@2019-09-10}"
    public TIntInstSet(String value) throws Exception {
        super(TInt::new, value);
    }

    //Array of strings "10@2019-09-08"
    public TIntInstSet(String[] values) throws Exception {
        super(TInt::new, values);
    }

    //Array of TIntInst
    public TIntInstSet(TIntInst[] values) throws Exception {
        super(TInt::new, values);
    }
}
