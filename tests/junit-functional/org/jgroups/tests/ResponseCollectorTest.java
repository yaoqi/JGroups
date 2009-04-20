
package org.jgroups.tests;


import junit.framework.TestCase;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.ResponseCollector;
import org.jgroups.util.Util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


/**
 * @author Bela Ban
 * @version $Id: ResponseCollectorTest.java,v 1.1.2.2 2009/04/20 13:00:19 belaban Exp $
 */
public class ResponseCollectorTest extends TestCase {

    public ResponseCollectorTest(String name) {
        super(name);
    }

    public void setUp() throws Exception {
        super.setUp();
    }


    public void tearDown() throws Exception {
        super.tearDown();
    }


    public void testAdd() {
        ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));
        coll.add(new IpAddress(1000), 1);
        System.out.println("coll = " + coll);
        assertEquals(3, coll.size());
        assertFalse(coll.hasAllResponses());
        coll.add(new IpAddress(3000), 3);
        coll.add(new IpAddress(2000), 2);
        System.out.println("coll = " + coll);
        assertEquals(3, coll.size());
        assertTrue(coll.hasAllResponses());
    }

    public void testAddNonExistentKeys() {
        ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(2));
        coll.add(new IpAddress(1000), 1);
        System.out.println("coll = " + coll);
        assertEquals(2, coll.size());
        assertFalse(coll.hasAllResponses());
        coll.add(new IpAddress(3000), 3); // will get dropped
        coll.add(new IpAddress(2000), 2);
        System.out.println("coll = " + coll);
        assertEquals(2, coll.size());
        assertTrue(coll.hasAllResponses());
    }


    public void testWaitForAllResponses() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));
        boolean rc=coll.waitForAllResponses(500);
        assertFalse(rc);

        new Thread() {
            public void run() {
                coll.add(new IpAddress(1000), 1);
                Util.sleep(500);
                coll.add(new IpAddress(2000), 2);
                coll.add(new IpAddress(3000), 3);
            }
        }.start();

        rc=coll.waitForAllResponses(700);
        System.out.println("coll = " + coll);
        assertTrue(rc);
        assertTrue(coll.hasAllResponses());
    }

    public void testWaitForAllResponsesAndTimeout() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));

        new Thread() {
            public void run() {
                coll.add(new IpAddress(1000), 1);
                Util.sleep(500);
                coll.add(new IpAddress(2000), 2);
                coll.add(new IpAddress(3000), 3);
            }
        }.start();

        boolean rc=coll.waitForAllResponses(400);
        System.out.println("coll = " + coll);
        assertFalse(rc);
        assertFalse(coll.hasAllResponses());
    }

    public void testWaitForAllResponsesAndStop() {
        final ResponseCollector<Integer> coll=new ResponseCollector<Integer>(createMembers(3));

        new Thread() {
            public void run() {
                Util.sleep(500);
                coll.add(new IpAddress(1000), 1);
                coll.stop();
            }
        }.start();

        boolean rc=coll.waitForAllResponses(700);
        System.out.println("coll = " + coll);
        assertFalse(rc);
        assertFalse(coll.hasAllResponses());
    }


    private static Collection<Address> createMembers(int num) {
        List<Address> retval=new ArrayList<Address>(num);
        int cnt=1000;
        for(int i=0; i < num; i++) {
            retval.add(new IpAddress(cnt));
            cnt+=1000;
        }
        return retval;
    }


    public static void main(String[] args) {
        String[] testCaseName={ResponseCollectorTest.class.getName()};
        junit.textui.TestRunner.main(testCaseName);
    }

}