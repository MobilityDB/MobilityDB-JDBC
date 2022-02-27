package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TIntSeqSet extends TSequenceSet<Integer, TInt> {

    public TIntSeqSet(TInt temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TIntSeqSet(String value) throws SQLException {
        super(TInt::new, value);
    }

    public TIntSeqSet(String[] values) throws SQLException {
        super(TInt::new, values);
    }

    public TIntSeqSet(TIntSeq[] values) throws SQLException {
        super(TInt::new, values);
    }

    @Override
    protected Temporal<Integer, TInt> convert(Object obj) {
        if (obj instanceof TIntSeqSet) {
            return (TIntSeqSet) obj;
        }
        return null;
    }
}
