package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TBoolSeq extends TSequence<Boolean, TBool> {

    public TBoolSeq(TBool temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    public TBoolSeq(String value) throws SQLException {
        super(TBool::new, value);
    }

    public TBoolSeq(String[] values) throws SQLException {
        super(TBool::new, values);
    }

    public TBoolSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TBool::new, values, lowerInclusive, upperInclusive);
    }

    public TBoolSeq(TBoolInst[] values) throws SQLException {
        super(TBool::new, values);
    }

    public TBoolSeq(TBoolInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TBool::new, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected Temporal<Boolean, TBool> convert(Object obj) {
        if (obj instanceof TBoolSeq) {
            return (TBoolSeq) obj;
        }
        return null;
    }
}
