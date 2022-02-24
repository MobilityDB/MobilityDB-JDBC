package edu.ulb.mobilitydb.jdbc.core;

import java.sql.SQLException;

/**
 * Class for MobilityDB exceptions
 */
public class MobilityDBException extends SQLException {
    public MobilityDBException(String reason) {
        super(reason);
    }
}
