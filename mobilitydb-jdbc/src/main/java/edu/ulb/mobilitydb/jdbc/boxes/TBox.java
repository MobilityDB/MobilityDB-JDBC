package edu.ulb.mobilitydb.jdbc.boxes;

import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;

import java.sql.SQLException;

@TypeName(name = "TBOX")
public class TBox extends DataType {
    public TBox() {
        super();
    }

    @Override
    public String toString() {
        return null;
    }

    @Override
    public void setValue(String value) throws SQLException {

    }
}
