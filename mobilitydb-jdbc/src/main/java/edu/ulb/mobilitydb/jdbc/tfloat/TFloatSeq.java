package edu.ulb.mobilitydb.jdbc.tfloat;

import edu.ulb.mobilitydb.jdbc.temporal.TSequence;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TFloatSeq extends TSequence<Float, TFloat> {

    public TFloatSeq(TFloat temporalDataType) throws SQLException {
        super(temporalDataType);
    }
    
    public TFloatSeq(String value) throws SQLException {
        super(TFloat::new, value);
    }

    public TFloatSeq(String[] values) throws SQLException {
        super(TFloat::new, values);
    }

    public TFloatSeq(String[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TFloat::new, values, lowerInclusive, upperInclusive);
    }

    public TFloatSeq(TFloatInst[] values) throws SQLException {
        super(TFloat::new, values);
    }

    public TFloatSeq(TFloatInst[] values, boolean lowerInclusive, boolean upperInclusive) throws SQLException {
        super(TFloat::new, values, lowerInclusive, upperInclusive);
    }

    @Override
    protected Temporal<Float, TFloat> convert(Object obj) {
        if (obj instanceof TFloatSeq) {
            return (TFloatSeq) obj;
        }
        return null;
    }
}
