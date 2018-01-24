package soap;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.jws.WebMethod;
import javax.jws.WebService;
import javax.naming.NamingException;

import generic.DatabaseConnectionManager;
import generic.GCDMessage;
import generic.JMSQueueManager;

@WebService
public class SOAPservice {
	
	@WebMethod
	@RolesAllowed({"ADMIN", "UNICO"})
	public int gcd() throws NamingException {
		int gcd = 0;
		// read top entry
		GCDMessage msg = JMSQueueManager.popFromQueue();
		
		if ( msg == null ) {
			System.out.println("Unable to read message from queue" );
			
			// ideally the method would return a status and not just a value
			// we have to imply from a zero return value that either there
			// was an error or no values in queue, no way to differentiate
			
			return 0;
		}
		long id = msg.getMsgId();
		int i1 = msg.getI1();
		int i2 = msg.getI2();
	
		// calculate 
		gcd = calcGcdValue( i1, i2);
		
		// update entry in table
		try {
			Connection dbConnection = DatabaseConnectionManager.getConnection();
		
			CallableStatement updateQuery = dbConnection.prepareCall("update my_table set gcd=:gcd where id=:id");
			updateQuery.setLong("id", id);
			updateQuery.setInt("gcd", gcd);
			if ( !updateQuery.execute() ) {
				// no database error but failed to update record
				// most likely is that record does not exist (anymore?)
				System.out.println("Failed to update database with result for Msg Id " + id );
				// allow call to return value even though DB not updated
			}
			
			dbConnection.close();
			
		} catch ( SQLException sqlExcaption) {
			// log an error message
			System.out.println("Error in database execution while updating Msg Id " + id );
			
			// at this point we have popped the top entry but failed
			// to update table with the result, value will not be retried.
			// Possible enhancement would be to attempt to push back onto queue without
			// adding a new DB entry.
		}
		
		return gcd;
	}
	
	@WebMethod
	@RolesAllowed({"ADMIN", "UNICO"})
	public List<Integer> gcdList() throws NamingException {
		ArrayList<Integer> results = new ArrayList<Integer>();
		
		try {
			Connection dbConnection = DatabaseConnectionManager.getConnection();
		
			// select values from DB
			Statement selectQuery = dbConnection.createStatement();
		
			// The statement below has been limited to 1000 rows
			// The assumption here is that we would not want to return an
			// unbounded list.
			// Using Oracle 12c SQL syntax.  Other DBs can apply similar limits
			// with different syntax.
			// If we wanted a DB agnostic SQL we could not limit the SQL but rather 
			// put the limit in the while loop below.
			// Note also I've hard-coded the 1000 row limit but could just have
			// easily been a configurable parameter.
			
			// Major assumption here is that we will not hit the limit which is imposed,
			// we'll only ever get the first set of records when the limit is hit,
			// ideally I'd return the last set of records up to a maximum but the 
			// exercise stated in order that they were added so fetching from start of
			// the list.
			boolean hasResults = selectQuery.execute("select gcd from my_table where gcd > 0 order asc by created fetch first 1000 rows only");
		
			while (hasResults) {
				ResultSet rs = selectQuery.getResultSet();

				// process result set
				results.add(rs.getInt(0));

				hasResults = selectQuery.getMoreResults();
			}
			
			dbConnection.close();
		} catch ( SQLException sqlException) {
			// log an error message
			System.out.println("Failed to access database" );
		}
		
		return results;

	}
	
	@WebMethod
	@RolesAllowed({"ADMIN", "UNICO"})
	public int gcdSum() throws NamingException {
		int result = 0;
		
		try {
			// start transaction
			Connection dbConnection = DatabaseConnectionManager.getConnection();
		
			// select all values from DB
			Statement selectQuery = dbConnection.createStatement();
		
		
			boolean hasResult = selectQuery.execute("select sum(gcd) from my_table");
		
			if (hasResult) {
				ResultSet rs = selectQuery.getResultSet();

				// process result set
				result = rs.getInt(0);
			}
			
			dbConnection.close();
		} catch ( SQLException sqlException) {
			// log an error message
			System.out.println("Failed to access database" );
		}
		
		return result;

	}
	
	public static int calcGcdValue( int a, int b ) {
		if ( a < 1 || b < 1 ) {
			// unable to calculate GCD when one of the values is zero
			return 0;
		}
		int larger = Math.max(a, b);
		int smaller = Math.min(a, b);
		
		while ( true ) {
			int divisor = larger/smaller;
			int remainder = larger - smaller*divisor;
			if ( remainder == 0 ) {
				return smaller; 
			} else {
				larger = smaller;
				smaller = remainder;
			}
		}
	}
}
