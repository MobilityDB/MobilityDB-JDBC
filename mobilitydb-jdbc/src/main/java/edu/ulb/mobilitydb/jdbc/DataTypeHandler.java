package edu.ulb.mobilitydb.jdbc;

import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.core.DataType;
import edu.ulb.mobilitydb.jdbc.core.TypeName;
import edu.ulb.mobilitydb.jdbc.time.Period;
import org.postgresql.PGConnection;

import java.sql.SQLException;
import java.util.ArrayList;

public enum DataTypeHandler {
    INSTANCE;

    private final ArrayList<Class<? extends DataType>> types;

    DataTypeHandler() {
        types = new ArrayList<>();
        types.add(Period.class);
        types.add(TBox.class);
    }

    public void registerTypes(PGConnection connection) {
        try {
            for (Class<? extends DataType> clazz : types) {
                if (clazz.isAnnotationPresent(TypeName.class)) {
                    connection.addDataType(clazz.getAnnotation(TypeName.class).name(), clazz);
                }
            }
        } catch (SQLException ex) {
            // log? throw?
        }
    }
}
