package edu.ulb.mobilitydb.jdbc.temporal;

import java.sql.SQLException;

public interface GetSingleTemporalValueFunction<V> {
    TemporalValue<V> run(String value) throws SQLException;
}
