
package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Allows a listener to be notified when message or state transfer events arrive.
 */
public interface MessageListener {
	/**
	 * Called when a message is received. 
	 * @param msg
	 */
    void          receive(Message msg);
    /**
     * Answers the group state; e.g., when joining.
     * @return byte[] 
     */
    byte[]        getState();
    /**
     * Sets the group state; e.g., when joining.
     * @param state
     */
    void          setState(byte[] state);

    /**
     * Allows an application to write a state through a provided OutputStream.
     *
     * @param ostream the OutputStream
     * @throws Exception if the streaming fails, any exceptions should be thrown so that the state requester can
     * re-throw them and let the caller know what happened
     * @see java.io.OutputStream#close()
     */
    public void getState(OutputStream ostream) throws Exception;


    /**
     * Allows an application to read a state through a provided InputStream.
     *
     * @param istream the InputStream
     * @throws Exception if the streaming fails, any exceptions should be thrown so that the state requester can
     * catch them and thus know what happened
     * @see java.io.InputStream#close()
     */
    public void setState(InputStream istream) throws Exception;
}
