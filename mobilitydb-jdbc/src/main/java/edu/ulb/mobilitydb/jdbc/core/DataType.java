package edu.ulb.mobilitydb.jdbc.core;

import org.postgresql.util.PGobject;

import java.sql.SQLException;

public abstract class DataType extends PGobject {
    protected DataType() {
        super();

        // register type
        Class<?> clazz = this.getClass();

        if (clazz.isAnnotationPresent(TypeName.class)) {
            setType(clazz.getAnnotation(TypeName.class).name());
        }
    }

    @Override
    public abstract String getValue();

    @Override
    public abstract void setValue(final String value) throws SQLException;
}
