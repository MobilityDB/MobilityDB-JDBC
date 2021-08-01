package edu.ulb.mobilitydb.jdbc;

import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.time.Period;
import edu.ulb.mobilitydb.jdbc.time.PeriodSet;
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
        types.add(TBox.class);
    }

    public void registerTypes(PGConnection connection) throws SQLException {
        for (Class<? extends DataType> clazz : types) {
            if (clazz.isAnnotationPresent(TypeName.class)) {
                connection.addDataType(clazz.getAnnotation(TypeName.class).name(), clazz);
            }
        }
    }
}
