package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TSequenceSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TBoolSeqSet extends TSequenceSet<Boolean, TBool> {
    public TBoolSeqSet(TBool temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TBoolSeqSet(String value) throws SQLException {
        super(TBool::new, value);
    }

    public TBoolSeqSet(String[] values) throws SQLException {
        super(TBool::new, values);
    }

    public TBoolSeqSet(TBoolSeq[] values) throws SQLException {
        super(TBool::new, values);
    }

    @Override
    protected Temporal<Boolean, TBool> convert(Object obj) {
        if (obj instanceof TBoolSeqSet) {
            return (TBoolSeqSet) obj;
        }
        return null;
    }
}
