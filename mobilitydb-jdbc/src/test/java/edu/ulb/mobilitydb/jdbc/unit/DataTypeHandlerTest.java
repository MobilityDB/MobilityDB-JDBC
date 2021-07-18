package edu.ulb.mobilitydb.jdbc.unit;

import edu.ulb.mobilitydb.jdbc.DataTypeHandler;
import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.time.Period;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.postgresql.PGConnection;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

public class DataTypeHandlerTest extends TestCase {
    @Test
    @DisplayName("Verifying register types works")
    public void testRegisterTypes() throws SQLException {
        PGConnection mockedConnection = mock(PGConnection.class);
        DataTypeHandler.INSTANCE.registerTypes(mockedConnection);
        verify(mockedConnection, atLeastOnce()).addDataType("period", Period.class);
        verify(mockedConnection, atLeastOnce()).addDataType("TBOX", TBox.class);
    }
}
