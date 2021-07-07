package edu.ulb.mobilitydb.jdbc;

import org.postgresql.PGConnection;
import org.postgresql.util.PGobject;

import java.sql.SQLException;

public enum DataTypeHandler {
    INSTANCE;

    private Class<? extends DataType>[] types;

    @SuppressWarnings("unchecked")
    DataTypeHandler() {
        types = new Class[]{
            Period.class
        };
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
