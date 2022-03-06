package edu.ulb.mobilitydb.jdbc.temporal;

import java.io.Serializable;
import java.sql.SQLException;

public interface GetSingleTemporalValueFunction<V extends Serializable> {
    TemporalValue<V> run(String value) throws SQLException;
}
