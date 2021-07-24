package edu.ulb.mobilitydb.jdbc.unit;

import edu.ulb.mobilitydb.jdbc.DataTypeHandler;
import edu.ulb.mobilitydb.jdbc.boxes.TBox;
import edu.ulb.mobilitydb.jdbc.time.Period;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;

import java.sql.SQLException;

import static org.mockito.Mockito.*;

class DataTypeHandlerTest {
    @Test
    @DisplayName("Verifying register types works")
    void testRegisterTypes() throws SQLException {
        PGConnection mockedConnection = mock(PGConnection.class);
        DataTypeHandler.INSTANCE.registerTypes(mockedConnection);
        verify(mockedConnection, atLeastOnce()).addDataType("period", Period.class);
        verify(mockedConnection, atLeastOnce()).addDataType("TBox", TBox.class);
    }
}
