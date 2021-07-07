package edu.ulb.mobilitydb.jdbc;

import org.postgresql.util.PGobject;

import java.sql.SQLException;

public abstract class DataType extends PGobject {
    DataType() {
        super();

        // register type
        Class<?> clazz = this.getClass();
        // TODO: If it is not present, throw?
        if (clazz.isAnnotationPresent(TypeName.class)) {
            setType(clazz.getAnnotation(TypeName.class).name());
        }
    }

    @Override
    public abstract String toString();

    @Override
    public abstract void setValue(final String value) throws SQLException;
}
