package generic;

import javax.jms.*;

// The GCDMessage class encapsulates the JMS Queue message
// This class populates the JMS Message class and also
// extracts the data from a JMS Message at the other end
// of the queue.
public class GCDMessage {
	// constants
	private final static String idProperty = "id";
	private final static String i1Property = "i1";
	private final static String i2Property = "i2";

	// message data
	private int msgId;
	private int i1;
	private int i2;

	public GCDMessage() {
		msgId = 0;
		i1 = 0;
		i2 = 0;
	}
	
	// This constructor bypasses the validation process
	// Assumed could be either removed or could an 
	// exception
	public GCDMessage( int id, int a, int b ) {
		msgId = id;
		i1 = a;
		i2 = b;
	}
	public int getMsgId() {
		return msgId;
	}

	public void setMsgId( int id ) {
		msgId = id;
	}

	public int getI1() {
		return i1;
	}

	public void setI1( int i ) {
		i1 = i;
	}

	public int getI2() {
		return i2;
	}

	public void setI2( int i ) {
		i2 = i;
	}
	
	public void populateMessage( Message msg ) throws JMSException {
			msg.setIntProperty( idProperty, msgId );
			msg.setIntProperty( i1Property, i1 );
			msg.setIntProperty( i2Property, i2 );
	}

	public boolean readMessage( Message msg ) {
		try {
			msgId = msg.getIntProperty( idProperty );
			i1 = msg.getIntProperty( i1Property );
			i2 = msg.getIntProperty( i2Property );
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return validate();
	}

	private boolean validate() {
		if ( msgId < 0 || i1 <=0 || i2 <= 0 ) {
			return false;
		}
		return true;
	}
}
