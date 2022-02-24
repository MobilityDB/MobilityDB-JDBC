package edu.ulb.mobilitydb.jdbc.temporal;

import java.time.OffsetDateTime;

/**
 * Represents a temporal value that consists in the value and a timestamp
 * @param <T> Could be Integer, Boolean, Float, String, etc
 */
public class TemporalValue<T> {
    private T value;
    private OffsetDateTime time;

    public TemporalValue(T value, OffsetDateTime time) {
        this.value = value;
        this.time = time;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public OffsetDateTime getTime() {
        return time;
    }

    public void setTime(OffsetDateTime time) {
        this.time = time;
    }

    /**
     * Returns the temporal value in MobilityDB format
     * eg: 10@2021-04-08 05:04:45+02
     * @return String
     */
    @Override
    public String toString() {
        return String.format("%s@%s", value, time);
    }
}
