package org.jgroups.tests;

import junit.framework.TestCase;
import org.jgroups.*;
import org.jgroups.protocols.UNICAST;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Util;
import org.jgroups.util.AgeOutCache;

import java.util.LinkedList;
import java.util.List;

/**
 * Tests sending of unicasts to members not in the group (http://jira.jboss.com/jira/browse/JGRP-357)
 * @author Bela Ban
 * @version $Id: UnicastEnableToTest.java,v 1.1.6.2 2009/04/15 07:18:44 belaban Exp $
 */
public class UnicastEnableToTest extends TestCase {
    JChannel c1=null, c2=null;
    AgeOutCache cache;

    public UnicastEnableToTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        c1=new JChannel("udp.xml");
        c1.connect("demo-group");
        UNICAST ucast=(UNICAST)c1.getProtocolStack().findProtocol(UNICAST.class);
        cache=ucast != null? ucast.getAgeOutCache() : null;
        if(cache != null)
            cache.setTimeout(1000);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        Util.close(c2, c1);
    }


    public void testUnicastMessageToUnknownMember() throws Exception {
        IpAddress addr=new IpAddress("127.0.0.1", 8976);
        System.out.println("sending message to non-existing destination " + addr);
        c1.send(new Message(addr, null, "Hello world"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assertEquals(1, cache.size());
        }
        Util.sleep(1500);
        if(cache != null) {
            assertEquals(0, cache.size());
        }
    }


    public void testUnicastMessageToExistingMember() throws Exception {
        c2=new JChannel("udp.xml");
        c2.connect("demo-group");
        assertEquals(2, c2.getView().size());
        MyReceiver receiver=new MyReceiver();
        c2.setReceiver(receiver);
        Address dest=c2.getLocalAddress();
        c1.send(new Message(dest, null, "hello"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assertEquals(0, cache.size());
        }
        Util.sleep(500);
        List list=receiver.getMsgs();
        System.out.println("channel2 received the following msgs: " + list);
        assertEquals(1, list.size());
        receiver.reset();
    }


    public void testUnicastMessageToLeftMember() throws Exception {
        c2=new JChannel("udp.xml");
        c2.connect("demo-group");
        assertEquals(2, c2.getView().size());
        Address dest=c2.getLocalAddress();
        c2.close();
        Util.sleep(100);
        c1.send(new Message(dest, null, "hello"));
        if(cache != null) {
            System.out.println("age out cache:\n" + cache);
            assertEquals(1, cache.size());
        }
        Util.sleep(1500);
        if(cache != null)
            assertEquals(0, cache.size());
    }


   


    private static class MyReceiver extends ExtendedReceiverAdapter {
        List<Message> msgs=new LinkedList<Message>();

        public void receive(Message msg) {
            msgs.add(msg);
        }

        List getMsgs() {
            return msgs;
        }

        void reset() {msgs.clear();}
    }


    public static void main(String[] args) {
        String[] testCaseName={UnicastEnableToTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }
}
