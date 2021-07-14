package edu.ulb.mobilitydb.jdbc.integration.time;

import edu.ulb.mobilitydb.jdbc.time.Period;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;

import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertAll;

public class PeriodTest extends TestCase {
    private Period period;

    public PeriodTest() throws SQLException {
        period = new Period("(2019-09-08 00:00:00+01, 2019-09-10 00:00:00+01]");
    }

    @Test
    @DisplayName("Verifying period constructor")
    public void testConstructor() {
        assertAll("Constructor values",
                () -> assertEquals(period.isLowerInclusive(), false),
                () -> assertEquals(period.isUpperInclusive(), true)
        );

    }
}
