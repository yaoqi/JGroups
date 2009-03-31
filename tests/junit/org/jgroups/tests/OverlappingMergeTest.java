package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Tests overlapping merges, e.g. A: {A,B}, B: {A,B} and C: {A,B,C}. Tests unicast as well as multicast seqno tables.<br/>
 * Related JIRA: https://jira.jboss.org/jira/browse/JGRP-940
 * @author Bela Ban
 * @version $Id: OverlappingMergeTest.java,v 1.1.2.1 2009/03/31 15:27:45 belaban Exp $
 */
public class OverlappingMergeTest extends ChannelTestBase {
    private JChannel a, b, c;
    private MyReceiver ra, rb, rc;

    protected void setUp() throws Exception {
        super.setUp();
        ra=new MyReceiver(); rb=new MyReceiver(); rc=new MyReceiver();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        Util.close(c,b,a);
    }

    /**
     * Verifies that unicasts are received correctly by all participants after an overlapping merge. The following steps
     * are executed:
     * <ol>
     * <li/>Group is {A,B,C}, disable shunning in all members. A is the coordinator
     * <li/>MERGE2 in all members is suspended: MERGE2.suspendMergeTask()
     * <li/>DISCARD is added to all members, just above the transport
     * <li/>VERIFY_SUSPECT is removed from all members
     * <li/>Everyone sends 5 unicast messages to everyone else
     * <li/>Everyone sends 5 multicasts
     * <li/>Set C's DISCARD protocol to discard all messages from A
     * <li/>This causes C to multicast a SUSPECT(A) message
     * <li/>A ignores it, but B decides to become the new coord
     * <li/>B mcasts view {B,C}
     * <li/>B and C install {B,C}
     * <li/>B and C trash the connection table for A in UNICAST
     * <li/>A ignores the view, it has still view {A,B,C}
     * <li/>MERGE2 resumes, and causes A and B as coords to create a new MergeView {A,B,C}
     * <li/>A sends a unicast to B and C. This should fail until JGRP-940 has been fixed !
     * <li/>Reason: B and C trashed A's conntables in UNICAST, but A didn't trash its conn tables for B and C, so
     * we have non-matching seqnos !
     * </ol>
     */
    public void testUnicastingAfterOverlappingMerge() throws Exception {
        a=createChannel(); a.setReceiver(ra);
        b=createChannel(); b.setReceiver(rb);
        c=createChannel(); c.setReceiver(rc);
        modifyConfigs(a, b, c);

        a.connect("testUnicastingAfterOverlappingMerge");
        b.connect("testUnicastingAfterOverlappingMerge");
        c.connect("testUnicastingAfterOverlappingMerge");

        View view=c.getView();
        assert view.size() == 3 : "view is " + view;

        sendMessages(a, b, c);

        Util.sleep(1000); // sleep a little to make sure async msgs have been received
        checkReceivedMessages(15, 15, ra, rb, rc);
    }


    private static void sendMessages(JChannel ... channels) throws Exception {
        // 1. send multicast messages
        for(JChannel ch: channels) {
            Address addr=ch.getLocalAddress();
            for(int i=1; i <= 5; i++)
                ch.send(null, null, "multicast msg #" + i + " from " + addr);
        }

        // 2. send unicast messages
        ArrayList<Address> mbrs=new ArrayList<Address>(channels[0].getView().getMembers());
        for(JChannel ch: channels) {
            Address addr=ch.getLocalAddress();
            for(Address dest: mbrs) {
                for(int i=1; i <=5; i++) {
                    ch.send(dest, null, "unicast msg #" + i + " from " + addr);
                }
            }
        }
    }

    private static void checkReceivedMessages(int num_mcasts, int num_ucasts, MyReceiver ... receivers) {
        for(MyReceiver receiver: receivers) {
            List<Message> mcasts=receiver.getMulticasts();
            List<Message> ucasts=receiver.getUnicasts();
            int mcasts_received=mcasts.size();
            int ucasts_received=ucasts.size();
            System.out.println("receiver " + receiver + ": mcasts=" + mcasts_received + ", ucasts=" + ucasts_received);
            assert mcasts_received == num_mcasts : "mcasts: " + mcasts;
            assert ucasts_received == num_ucasts : "ucasts: " + ucasts;
        }
    }

    private static void modifyConfigs(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();

            FD fd=(FD)stack.findProtocol(FD.class);
            if(fd != null) {
                fd.setShun(false);
            }
            
            FD_ALL fd_all=(FD_ALL)stack.findProtocol(FD_ALL.class);
            if(fd_all != null) {
                fd_all.setShun(false);
            }

            MERGE2 merge=(MERGE2)stack.findProtocol("MERGE2");
            if(merge == null)
                throw new IllegalStateException("MERGE2 is required");
            merge.setMinInterval(5000);
            merge.setMaxInterval(10000);
            merge.suspendMergeTask();
            
            DISCARD discard=new DISCARD(); // insert DISCARD just above the transport
            stack.insertProtocol(discard, ProtocolStack.ABOVE, stack.getTransport());

            stack.removeProtocol("VERIFY_SUSPECT");
        }
    }


    private static class MyReceiver extends ReceiverAdapter {
        final List<Message> mcasts=new ArrayList<Message>(20);
        final List<Message> ucasts=new ArrayList<Message>(20);

        public void receive(Message msg) {
            Address dest=msg.getDest();
            boolean mcast=dest == null;
            if(mcast)
                mcasts.add(msg);
            else
                ucasts.add(msg);
            // System.out.println("received " + (mcast? "mcast" : "ucast") + " msg from " + msg.getSrc());
        }

        public List<Message> getMulticasts() { return mcasts; }

        public List<Message> getUnicasts() { return ucasts; }

    }



}