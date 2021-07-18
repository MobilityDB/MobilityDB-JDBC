package edu.ulb.mobilitydb.jdbc.unit.time;


import edu.ulb.mobilitydb.jdbc.time.Period;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertAll;

public class PeriodTest extends TestCase {

    @Test
    @DisplayName("Verifying period constructor")
    public void testConstructor() throws SQLException {
        Period period = new Period("(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]");
        assertAll("Constructor values",
            () -> assertFalse(period.isLowerInclusive()),
            () -> assertTrue(period.isUpperInclusive())
        );
    }
}
