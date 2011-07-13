package org.jgroups;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Bela Ban
 */
public class ReceiverAdapter implements Receiver {

    public void receive(Message msg) {
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    public void getState(OutputStream ostream) throws Exception {
    }

    public void setState(InputStream istream) throws Exception {
    }

    public void viewAccepted(View view) {
    }

    public void suspect(Address mbr) {
    }

    public void block() {
    }

    public void unblock() {
    }
}
