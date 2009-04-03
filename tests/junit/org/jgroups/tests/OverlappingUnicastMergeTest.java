package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Tests overlapping merges, e.g. A: {A,B}, B: {A,B} and C: {A,B,C}. Tests unicast tables<br/>
 * Related JIRA: https://jira.jboss.org/jira/browse/JGRP-940
 * @author Bela Ban
 * @version $Id: OverlappingUnicastMergeTest.java,v 1.1.2.2 2009/04/03 14:24:25 belaban Exp $
 */
public class OverlappingUnicastMergeTest extends ChannelTestBase {
    private JChannel a, b, c;
    private MyReceiver ra, rb, rc;

    protected void setUp() throws Exception {
        super.setUp();
        ra=new MyReceiver("A"); rb=new MyReceiver("B"); rc=new MyReceiver("C");
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
     * <li/>MERGE2 is removed from all members
     * <li/>VERIFY_SUSPECT is removed from all members
     * <li/>Everyone sends 5 unicast messages to everyone else
     * <li/>A VIEW(B,C) is injected into B and C
     * <li/>B and C install {B,C}
     * <li/>B and C trash the connection table for A in UNICAST
     * <li/>A still has view {A,B,C} and all connection tables intact in UNICAST
     * <li/>We now inject a MERGE(A,B) event into A. This should cause A and B as coords to create a new MergeView {A,B,C}
     * <li/>The merge already fails because the unicast between A and B fails due to the reason given below !
     *      Once this is fixed, the next step below should work, too !
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
        assertEquals("view is " + view, 3, view.size());
        sendMessages(a, b, c);
        Util.sleep(1000); // sleep a little to make sure async msgs have been received
        checkReceivedMessages(15, ra, rb, rc);

        // Inject view {B,C} into B and C:
        View new_view=Util.createView(b.getLocalAddress(), 10, b.getLocalAddress(), c.getLocalAddress());
        injectView(new_view, b, c);

        System.out.println("A's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());
        assertEquals("A's view is " + a.getView(), 3, a.getView().size());
        assertEquals("B's view is " + b.getView(), 2, b.getView().size());
        assertEquals("C's view is " + c.getView(), 2, c.getView().size());

        ra.clear(); rb.clear(); rc.clear();
        sendMessages(a, b, c);
        Util.sleep(1000); // sleep a little to make sure async msgs have been received
        checkReceivedMessages(15, ra, rb, rc);

        // Inject view {A} into A, B and C:
        new_view=Util.createView(a.getLocalAddress(), 10, a.getLocalAddress());
        injectView(new_view, a, b, c);

        System.out.println("A's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());
        assertEquals("A's view is " + a.getView(), 1, a.getView().size());
        assertEquals("B's view is " + b.getView(), 1, b.getView().size());
        assertEquals("C's view is " + c.getView(), 1, c.getView().size());

        ra.clear(); rb.clear(); rc.clear();
        sendMessages(a, b, c);
        Util.sleep(1000); // sleep a little to make sure async msgs have been received
        checkReceivedMessages(15, ra, rb, rc);
    }


    private static JChannel determineMergeLeader(JChannel ... coords) {
        Membership tmp=new Membership();
        for(JChannel ch: coords) {
            tmp.add(ch.getLocalAddress());
        }
        tmp.sort();
        Address  merge_leader=tmp.elementAt(0);
        for(JChannel ch: coords) {
            if(ch.getLocalAddress().equals(merge_leader))
                return ch;
        }
        return null;
    }

    private static void injectView(View view, JChannel ... channels) {
        for(JChannel ch: channels) {
            ch.down(new Event(Event.VIEW_CHANGE, view));
            ch.up(new Event(Event.VIEW_CHANGE, view));
        }
    }

    private static void injectMergeEvent(Event evt, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.up(evt);
        }
    }


    private static void sendMessages(JChannel ... channels) throws Exception {
        // 1. send unicast messages
        ArrayList<Address> mbrs=new ArrayList<Address>(channels[0].getView().getMembers());
        for(JChannel ch: channels) {
            Address addr=ch.getLocalAddress();
            for(Address dest: mbrs) {
                ch.down(new Event(Event.ENABLE_UNICASTS_TO, dest));
                for(int i=1; i <=5; i++) {
                    ch.send(dest, null, "unicast msg #" + i + " from " + addr);
                }
            }
        }
    }

    private static void checkReceivedMessages(int num_ucasts, MyReceiver ... receivers) {
        for(MyReceiver receiver: receivers) {
            List<Message> ucasts=receiver.getUnicasts();
            int ucasts_received=ucasts.size();
            System.out.println("receiver " + receiver + ": ucasts=" + ucasts_received);
            assertEquals("ucasts: " + ucasts, ucasts_received, num_ucasts);

        }
    }

    private static void modifyConfigs(JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();

            FD fd=(FD)stack.findProtocol(FD.class);
            if(fd != null)
                fd.setShun(false);

            FD_ALL fd_all=(FD_ALL)stack.findProtocol(FD_ALL.class);
            if(fd_all != null)
                fd_all.setShun(false);

            stack.removeProtocol("MERGE2");

            stack.removeProtocol("VERIFY_SUSPECT");
        }
    }



    private static class MyReceiver extends ReceiverAdapter {
        final String name;
        final List<Message> ucasts=new ArrayList<Message>(20);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Address dest=msg.getDest();
            boolean mcast=dest == null;
            if(!mcast)
                ucasts.add(msg);
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "] " + new_view);
        }

        public List<Message> getUnicasts() { return ucasts; }
        public void clear() {ucasts.clear();}

        public String toString() {
            return name;
        }
    }



}