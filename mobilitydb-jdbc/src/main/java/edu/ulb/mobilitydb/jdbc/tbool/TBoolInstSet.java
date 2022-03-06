package edu.ulb.mobilitydb.jdbc.tbool;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TBoolInstSet extends TInstantSet<Boolean> {
    public TBoolInstSet(String value) throws SQLException {
        super(value, TBool::getSingleTemporalValue);
    }

    public TBoolInstSet(String[] values) throws SQLException {
        super(values, TBool::getSingleTemporalValue);
    }

    public TBoolInstSet(TBoolInst[] values) throws SQLException {
        super(values);
    }

    @Override
    protected Temporal<Boolean> convert(Object obj) {
        if (obj instanceof TBoolInstSet) {
            return (TBoolInstSet) obj;
        }
        return null;
    }
}
