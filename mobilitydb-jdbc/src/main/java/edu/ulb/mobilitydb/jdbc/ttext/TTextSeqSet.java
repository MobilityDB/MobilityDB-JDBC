package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

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

    @Override
    protected Temporal<String> convert(Object obj) {
        if (obj instanceof TTextSeqSet) {
            return (TTextSeqSet) obj;
        }
        return null;
    }
}
