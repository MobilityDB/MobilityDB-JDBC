package edu.ulb.mobilitydb.jdbc.temporal;

import java.time.OffsetDateTime;

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

    @Override
    public String toString() {
        return String.format("%s@%s", value, time);
    }
}
