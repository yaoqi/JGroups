package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.FD;
import org.jgroups.protocols.FD_ALL;
import org.jgroups.protocols.MERGE2;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Tests overlapping merges, e.g. A: {A,B}, B: {A,B} and C: {A,B,C}. Tests unicast as well as multicast seqno tables.<br/>
 * Related JIRA: https://jira.jboss.org/jira/browse/JGRP-940
 * @author Bela Ban
 * @version $Id: OverlappingMergeTest.java,v 1.1.2.2 2009/03/31 16:34:14 belaban Exp $
 */
public class OverlappingMergeTest extends ChannelTestBase {
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
     * <li/>Everyone sends 5 multicasts
     * <li/>A SUSPECT(A) event is injected into B's stack (GMS). This causes a new view {B,C} to be multicast by B
     * <li/>B and C install {B,C}
     * <li/>B and C trash the connection table for A in UNICAST
     * <li/>A ignores the view, it still has view {A,B,C} and all connection tables intact in UNICAST
     * <li/>We now inject a MERGE(A,B) event into A. This should ause A and B as coords to create a new MergeView {A,B,C}
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
        assert view.size() == 3 : "view is " + view;

        sendMessages(a, b, c);

        Util.sleep(1000); // sleep a little to make sure async msgs have been received
        checkReceivedMessages(15, 15, ra, rb, rc);

        // send a SUSPECT(A) event up B's stack, B will become coord and install {B,C} in itself and C
        injectSuspectEvent(b, a.getLocalAddress());
        Util.sleep(1000);
        System.out.println("A's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());
        assert a.getView().size() == 3 : "A's view is " + a.getView();
        assert b.getView().size() == 2 : "B's view is " + b.getView();
        assert c.getView().size() == 2 : "C's view is " + c.getView();

        // resume merging
        System.out.println("Merging started");
        Vector<Address> coords=new Vector<Address>(2);
        coords.add(a.getLocalAddress()); coords.add(b.getLocalAddress());
        Event merge_evt=new Event(Event.MERGE, coords);
        injectMergeEvent(merge_evt, a);

        for(int i=0; i < 10; i++) {
            if(a.getView().size() == 3 && b.getView().size() == 3 && c.getView().size() == 3)
                break;
            Util.sleep(1000);
        }
        System.out.println("checking views after merge:");

        System.out.println("A's view: " + a.getView());
        System.out.println("B's view: " + b.getView());
        System.out.println("C's view: " + c.getView());
        assert a.getView().size() == 3 : "A's view is " + a.getView();
        assert b.getView().size() == 3 : "B's view is " + b.getView();
        assert c.getView().size() == 3 : "C's view is " + c.getView();

        ra.clear(); rb.clear(); rc.clear();
        System.out.println("Sending messages after merge");
        sendMessages(a, b, c);
        Util.sleep(1000); // sleep a little to make sure async msgs have been received
        checkReceivedMessages(15, 15, ra, rb, rc);
    }

    private static void injectSuspectEvent(JChannel ch, Address suspected_mbr) {
        GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
        gms.up(new Event(Event.SUSPECT, suspected_mbr));
    }

    private static void injectMergeEvent(Event evt, JChannel ... channels) {
        for(JChannel ch: channels) {
            GMS gms=(GMS)ch.getProtocolStack().findProtocol(GMS.class);
            gms.up(evt);
        }
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
        final List<Message> mcasts=new ArrayList<Message>(20);
        final List<Message> ucasts=new ArrayList<Message>(20);

        public MyReceiver(String name) {
            this.name=name;
        }

        public void receive(Message msg) {
            Address dest=msg.getDest();
            boolean mcast=dest == null;
            if(mcast)
                mcasts.add(msg);
            else
                ucasts.add(msg);
            // System.out.println("received " + (mcast? "mcast" : "ucast") + " msg from " + msg.getSrc());
        }

        public void viewAccepted(View new_view) {
            System.out.println("[" + name + "] " + new_view);
        }

        public List<Message> getMulticasts() { return mcasts; }
        public List<Message> getUnicasts() { return ucasts; }
        public void clear() {mcasts.clear(); ucasts.clear();}
    }



}