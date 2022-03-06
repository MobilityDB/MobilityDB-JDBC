package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;

import java.sql.SQLException;

public class TTextSeqSet extends TSequenceSet<String> {
    public TTextSeqSet(String value) throws SQLException {
        super(value, TText::getSingleTemporalValue);
    }

    public TTextSeqSet(String[] values) throws SQLException {
        super(values, TText::getSingleTemporalValue);
    }

    public TTextSeqSet(TTextSeq[] values) throws SQLException {
        super(values, TText::getSingleTemporalValue);
    }
}
