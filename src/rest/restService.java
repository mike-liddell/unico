package rest;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.annotation.security.RolesAllowed;
import javax.naming.NamingException;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import generic.DatabaseConnectionManager;
import generic.GCDMessage;
import generic.JMSQueueManager;

@Path("restServices")

public class restService {
	@POST
	@Path("push")
	@Consumes("application/json")
	@Produces("application/json")
	@RolesAllowed({"ADMIN", "UNICO"})
	public String push( @PathParam("i1") int i1, @PathParam("i2") int i2) throws NamingException {
		int msgId = 0;
		
		// check input values are within valid range for GCD calculation
		if ( i1 < 1 || i2 < 1 ) {
			System.out.println("Input values are not within a valid range for Msg Id " + msgId );
			return "Failure";
		}
		
		try {
			Connection dbConnection = DatabaseConnectionManager.getConnection();
		
			boolean insertResult = false;
		
			// try three times to insert record,
			// assumption here is that if record fails to insert it is because
			// of a duplicate (random) key so trying again should succeed
			for ( int i=0; i<3; i++ ) {
				// Using a random number for the ID.
				// Could use an auto-increment sequence in the DB but this has
				// performance issues when running with a distributed DB such as
				// Oracle RAC.  Using a random number, and optimistically, assuming
				// there will not be a collision.  If there is try again up to 3 times.
				Random rand = new Random();
				msgId = rand.nextInt();
			
				// insert into DB
				// Using DB timestamp to make time of entry into DB,
				// does not depend on local server or sessions clock,
				// although it does use the local sessions timezone
				CallableStatement insertQuery = dbConnection.prepareCall("insert into my_table (id, i1, i2, gcd, created) values (:id, :i1, :i2, :gcd, CURRENT_TIMESTAMP)");
				insertQuery.setInt("id", msgId);
				insertQuery.setInt("i1", i1);
				insertQuery.setInt("i2", i2);
				insertQuery.setInt("gcd", 0);
				insertResult = insertQuery.execute();
						
				if ( insertResult ) {
					// success!!
					break;
				} else {
					// log failed attempt
					System.out.println("Collision with Msg Id " + msgId );
				}
			
			}	
			
			if ( !insertResult ) {
				// after 3 attempts still failed to insert result
				System.out.println("Failed to insert Msg Id " + msgId );
				return "Failure";
			}
			dbConnection.close();
		} catch (SQLException e) {
			System.out.println("Database error, Failed to insert Msg Id " + msgId );
			return "Failure";
		}
		
		// push to JMS
		GCDMessage jmsMessage = new GCDMessage(msgId, i1, i2 );
		
		if ( JMSQueueManager.pushToQueue(jmsMessage) ) {
			return "Success";
		} else {
			return "Failure";
		}
	}
	
	
	@POST  
	@Consumes("application/json")
	@Produces("application/json")
	@RolesAllowed({"ADMIN", "UNICO"})
	public List<Integer> list() throws NamingException {
		// create result array so that something can be returned in case of error
		ArrayList<Integer> results = new ArrayList<Integer>();
		
		try {
			Connection dbConnection = DatabaseConnectionManager.getConnection();
			
			// select all values from DB
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
			boolean hasResults = selectQuery.execute("select i1, i2 from my_table order asc by created fetch first 1000 rows only");
		
			while (hasResults) {
				ResultSet rs = selectQuery.getResultSet();

				// process result set
				results.add(rs.getInt(0));
				results.add(rs.getInt(1));

				hasResults = selectQuery.getMoreResults();
			}
		} catch ( SQLException sqlException) {
			System.out.println("Database error while reading list" );
			
			// decided to allow method to return from here with whatever results
			// had already been read.
			// Alternatively could explicitly return an empty list
		}
		
		return results;
	}

}
