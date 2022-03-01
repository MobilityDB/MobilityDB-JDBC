package edu.ulb.mobilitydb.jdbc.ttext;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TTextSeq extends TSequence<String, TText> {

    public TTextSeq(TText temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TTextSeq(String value) throws SQLException {
        super(TText::new, value);
    }

    public TTextSeq(String[] values) throws SQLException {
        super(TText::new, values);
    }

    public TTextSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TText::new, values, lowerInclusive, upperInclusive);
    }

    public TTextSeq(TTextInst[] values) throws SQLException {
        super(TText::new, values);
    }

    public TTextSeq(TTextInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TText::new, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected Temporal<String, TText> convert(Object obj) {
        if (obj instanceof TTextSeq) {
            return (TTextSeq) obj;
        }
        return null;
    }
}
