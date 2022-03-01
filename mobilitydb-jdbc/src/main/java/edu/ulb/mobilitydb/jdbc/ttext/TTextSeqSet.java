package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TTextSeqSet extends TSequenceSet<String, TText> {

    public TTextSeqSet(TText temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TTextSeqSet(String value) throws SQLException {
        super(TText::new, value);
    }

    public TTextSeqSet(String[] values) throws SQLException {
        super(TText::new, values);
    }

    public TTextSeqSet(TTextSeq[] values) throws SQLException {
        super(TText::new, values);
    }

    @Override
    protected Temporal<String, TText> convert(Object obj) {
        if (obj instanceof TTextSeqSet) {
            return (TTextSeqSet) obj;
        }
        return null;
    }
}
