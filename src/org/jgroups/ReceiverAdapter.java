package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Bela Ban
 */
public class ReceiverAdapter implements Receiver {

    /** {@inheritDoc} */
    public void receive(Message msg) {
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    /** {@inheritDoc} */
    public void getState(OutputStream ostream) throws Exception {
    }

    /** {@inheritDoc} */
    public void setState(InputStream istream) throws Exception {
    }

    /** {@inheritDoc} */
    public void viewAccepted(View view) {
    }

    public void suspect(Address mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }
}
