package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TIntSeq extends TSequence<Integer, TInt> {

    public TIntSeq(TInt temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TIntSeq(String value) throws SQLException {
        super(TInt::new, value);
    }

    public TIntSeq(String[] values) throws SQLException {
        super(TInt::new, values);
    }

    public TIntSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TInt::new, values, lowerInclusive, upperInclusive);
    }

    public TIntSeq(TIntInst[] values) throws SQLException {
        super(TInt::new, values);
    }

    public TIntSeq(TIntInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
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
