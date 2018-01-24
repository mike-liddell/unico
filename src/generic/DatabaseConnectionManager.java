package generic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

// Simple table schema used (assuming Oracle)
// Table: my_table
// Columns: 
//		NUMBER id
//		NUMBER i1
//		NUMBER i2
//		NUMBER gcd
//		TIMESTAMP created

public class DatabaseConnectionManager {

	// SQLException will be caught and handled by the caller
	// Explicitly not catching the NamingException.  If there is an issue with the
	// naming of the DB connection allow to fail and flow up the stack
	public static Connection getConnection() throws SQLException, NamingException {
		Context context = new InitialContext();
		String connectionId = (String)context.lookup("databaseConnection");				

		Connection dbConnection = DriverManager.getConnection( connectionId );
		// no need for complex transactions, allow all statements to auto-commit
		dbConnection.setAutoCommit(true);
		return dbConnection;
	}
}
