package org.jgroups.tests;

import org.jgroups.ChannelException;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.ReceiverAdapter;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Tests state transfer API (including exception handling)
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class StateTransferTest2 extends ChannelTestBase {
    protected JChannel c1, c2;

    @BeforeMethod protected void setup() throws Exception {
        c1=createChannel(true, 2, "A");
        c2=createChannel(c1, "B");
        c1.connect("StateTransferTest2");
        c2.connect("StateTransferTest2");
        assert c2.getView().size() == 2 : "view of C2 is " + c2.getView();
    }

    @AfterMethod protected void destroy() {Util.close(c2,c1);}

    
    public void testSuccessfulStateTransfer() throws ChannelException {
        StateHandler sh1=new StateHandler("Bela", false, false),
          sh2=new StateHandler(null, false, false);
        c1.setReceiver(sh1);
        c2.setReceiver(sh2);
        boolean rc=c2.getState(null, 10000);
        assert rc;
        Object state=sh2.getReceivedState();
        System.out.println("state = " + state);
        assert state != null && state.equals("Bela");
    }



    protected static class StateHandler extends ReceiverAdapter {
        protected final boolean get_error;
        protected final boolean set_error;
        protected final Object state_to_send;
        protected       Object received_state=null;

        public StateHandler(Object state_to_send, boolean get_error, boolean set_error) {
            this.state_to_send=state_to_send;
            this.get_error=get_error;
            this.set_error=set_error;
        }

        public Object getReceivedState() {
            return received_state;
        }

        public byte[] getState() {
            if(get_error)
                throw new RuntimeException("state could not be serialized");
            try {
                return Util.objectToByteBuffer(state_to_send);
            }
            catch(Exception e) {
                throw new RuntimeException("failed getting state", e);
            }
        }

        public void getState(OutputStream ostream) {
            if(get_error)
                throw new RuntimeException("state could not be serialized");
            DataOutputStream out=new DataOutputStream(ostream);
            try {
                Util.objectToStream(state_to_send, out);
            }
            catch(Exception e) {
                throw new RuntimeException("failed getting state", e);
            }
            finally {
                Util.close(out);
            }
        }

        public void setState(InputStream istream) {
            if(set_error)
                throw new RuntimeException("state could not be set");
            DataInputStream in=new DataInputStream(istream);
            try {
                received_state=Util.objectFromStream(in);
            }
            catch(Exception e) {
                throw new RuntimeException("failed setting state", e);
            }
        }

        public void setState(byte[] state) {
            if(set_error)
                throw new RuntimeException("state could not be set");
            try {
                this.received_state=Util.objectFromByteBuffer(state);
            }
            catch(Exception e) {
                throw new RuntimeException("failed setting state", e);
            }
        }
    }

}