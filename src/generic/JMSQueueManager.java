package generic;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;

public class JMSQueueManager {

	// static initialisations
	static private Context context = null; 
	static private ConnectionFactory connectionFactory = null; 
	static private Destination queue = null; 

	private static synchronized boolean initialise() {
		if ( context != null ) {
			// ideally check if queue is still up and running
			return true;
		}
		try {
			context = new InitialContext();		
			connectionFactory = (ConnectionFactory)context.lookup("/ConnectionFactory");		
			queue = (Destination)context.lookup("queue/GCDQueue");
		} catch (NamingException e) {
			// reset to allow next call to try to initialise
			context = null;
			connectionFactory = null;
			queue = null;
			return false;
		}
		return true;
	}
	
	public static boolean pushToQueue( GCDMessage msgContainer )
	{
		if ( !initialise() ) {
			return false;
		}
			
		// push message to queue
		Connection connection;
		try {
			connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE); 
			MessageProducer producer = session.createProducer(queue); 
			Message sntMessage =session.createMessage(); 
			msgContainer.populateMessage( sntMessage );
			producer.send(sntMessage); 
			System.out.println("Sent successfully Msg Id " + msgContainer.getMsgId() ); 
			connection.close();
		} catch (JMSException e) {
			
			System.out.println("Failed to send Msg Id " + msgContainer.getMsgId() );
			return false;
		} 
		return true;
	}

	// If there are no messages on the queue or there is an error the
	// result from this method is the same (null returned).
	// Differentiation could be included if the service which calls the 
	// method had mechanisms for behaving differently depending on the result.
	public static GCDMessage popFromQueue() {
		GCDMessage msgContainer = null;
		// pop message from queue

		if ( !initialise() ) {
			return null;
		}

		try {
			// create connection the JMS queue
			Connection connection = connectionFactory.createConnection();
			Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE); 
			MessageConsumer consumer = session.createConsumer(queue); 
			connection.start();
			
			// don't wait for a message to be added to the queue.
			// If there are no messages on the queue then just return straight
			// away.
			Message rcvMessage = consumer.receiveNoWait(); 
			if ( rcvMessage == null ) {
				// no message available
				System.out.println("No messages available in queue" );
				return null;
			}
			msgContainer = new GCDMessage();
			msgContainer.readMessage( rcvMessage );
			System.out.println("Received Msg Id = " + msgContainer.getMsgId() ); 
			connection.close();
		} catch (JMSException e) {
			System.out.println( "Failed to read queue" );
			return null;
		}
		
		return msgContainer;
	}
}
