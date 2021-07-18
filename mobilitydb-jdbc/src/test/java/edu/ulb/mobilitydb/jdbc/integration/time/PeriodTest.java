package edu.ulb.mobilitydb.jdbc.integration.time;

import edu.ulb.mobilitydb.jdbc.time.Period;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

class PeriodTest {
    private Period period;

    PeriodTest() throws SQLException {
        period = new Period("(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]");
    }

    @Test
    @DisplayName("Verifying period constructor")
    void testConstructor() {
        assertAll("Constructor values",
                () -> assertEquals(false, period.isLowerInclusive()),
                () -> assertEquals(true, period.isUpperInclusive())
        );

    }
}
