package edu.ulb.mobilitydb.jdbc.tint;

import edu.ulb.mobilitydb.jdbc.temporal.TInstantSet;
import edu.ulb.mobilitydb.jdbc.temporal.Temporal;

import java.sql.SQLException;

public class TIntInstSet extends TInstantSet<Integer, TInt> {

    //READ
    public TIntInstSet(TInt temporalDataType) throws SQLException {
        super(temporalDataType);
    }

    //WRITE
    //String of "{10@2019-09-08, 20@2019-09-09, 20@2019-09-10}"
    public TIntInstSet(String value) throws SQLException {
        super(TInt::new, value);
    }

    //Array of strings "10@2019-09-08"
    public TIntInstSet(String[] values) throws SQLException {
        super(TInt::new, values);
    }

    //Array of TIntInst
    public TIntInstSet(TIntInst[] values) throws SQLException {
        super(TInt::new, values);
    }

    @Override
    protected Temporal<Integer, TInt> convert(Object obj) {
        if (obj instanceof TIntInstSet) {
            return (TIntInstSet) obj;
        }
        return null;
    }
}
