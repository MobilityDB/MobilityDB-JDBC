package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

public class TIntSeq extends TSequence<Integer, TInt> {

    public TIntSeq(TInt temporalDataType) throws Exception {
        super(temporalDataType);
    }

    public TIntSeq(String value) throws Exception {
        super(TInt::new, value);
    }

    public TIntSeq(String[] values) throws Exception {
        super(TInt::new, values);
    }

    public TIntSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws Exception {
        super(TInt::new, values, lowerInclusive, upperInclusive);
    }

    public TIntSeq(TIntInst[] values) throws Exception {
        super(TInt::new, values);
    }

    public TIntSeq(TIntInst[] values, boolean lowerInclusive, boolean upperInclusive) throws Exception {
        super(TInt::new, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected Temporal<Integer, TInt> convert(Object obj) {
        if (obj instanceof TIntSeq) {
            return (TIntSeq) obj;
        }
        return null;
    }
}
