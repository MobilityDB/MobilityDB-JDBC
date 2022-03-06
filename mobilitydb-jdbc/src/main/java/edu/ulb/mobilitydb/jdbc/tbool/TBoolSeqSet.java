package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;

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
}
