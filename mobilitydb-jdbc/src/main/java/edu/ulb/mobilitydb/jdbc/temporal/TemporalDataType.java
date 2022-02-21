package edu.ulb.mobilitydb.jdbc.temporal;

public interface TemporalDataType<T> {

    TemporalType getTemporalType();

    TemporalValue<T> getSingleTemporalValue(String value);

}
