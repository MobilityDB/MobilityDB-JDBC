package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TBoolSeqSet extends TSequenceSet<Boolean> {
    public TBoolSeqSet(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue);
    }

    public TBoolSeqSet(String[] values) throws SQLException {
        super(values, TBool::getSingleTemporalValue);
    }

    public TBoolSeqSet(TBoolSeq[] values) throws SQLException {
        super(values, TBool::getSingleTemporalValue);
    }

    @Override
    protected Temporal<Boolean> convert(Object obj) {
        if (obj instanceof TBoolSeqSet) {
            return (TBoolSeqSet) obj;
        }
        return null;
    }
}
