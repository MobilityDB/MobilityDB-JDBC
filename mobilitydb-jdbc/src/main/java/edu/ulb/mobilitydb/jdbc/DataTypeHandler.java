package edu.ulb.mobilitydb.jdbc;

import edu.ulb.mobilitydb.jdbc.boxes.STBox;
import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.tbool.TBool;
import edu.ulb.mobilitydb.jdbc.tfloat.TFloat;
import edu.ulb.mobilitydb.jdbc.tpoint.tgeom.TGeomPoint;
import edu.ulb.mobilitydb.jdbc.time.Period;
import edu.ulb.mobilitydb.jdbc.time.PeriodSet;
import edu.ulb.mobilitydb.jdbc.time.TimestampSet;
import edu.ulb.mobilitydb.jdbc.tint.TInt;
import edu.ulb.mobilitydb.jdbc.ttext.TText;
import org.postgresql.PGConnection;

import java.sql.SQLException;
import java.util.ArrayList;

public enum DataTypeHandler {
    INSTANCE;

    private final ArrayList<Class<? extends DataType>> types;

    DataTypeHandler() {
        types = new ArrayList<>();
        types.add(Period.class);
        types.add(PeriodSet.class);
        types.add(TimestampSet.class);
        types.add(TBox.class);
        types.add(STBox.class);
        types.add(TInt.class);
        types.add(TBool.class);
        types.add(TFloat.class);
        types.add(TText.class);
        types.add(TGeomPoint.class);
    }

    public void registerTypes(PGConnection connection) throws SQLException {
        for (Class<? extends DataType> clazz : types) {
            if (clazz.isAnnotationPresent(TypeName.class)) {
                connection.addDataType(clazz.getAnnotation(TypeName.class).name(), clazz);
            }
        }
    }
}
