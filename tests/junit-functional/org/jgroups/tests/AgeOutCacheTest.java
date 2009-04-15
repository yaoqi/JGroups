// $Id: AgeOutCacheTest.java,v 1.1.2.1 2009/04/15 06:11:25 belaban Exp $
package org.jgroups.tests;


import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Message;
import org.jgroups.stack.AckSenderWindow;
import org.jgroups.stack.StaticInterval;
import org.jgroups.util.Util;
import org.jgroups.util.AgeOutCache;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;


/**
 * Test cases for AckSenderWindow
 * @author Bela Ban
 */
public class AgeOutCacheTest extends TestCase {
    AgeOutCache<Integer> cache;
    ScheduledExecutorService timer=Executors.newScheduledThreadPool(1);

    public AgeOutCacheTest(String name) {
        super(name);
    }

    protected void setUp() throws Exception {
        super.setUp();
        timer=Executors.newScheduledThreadPool(1);
    }

    protected void tearDown() throws Exception {
        timer.shutdownNow();
        super.tearDown();
    }

    public void testExpiration() {
        cache=new AgeOutCache<Integer>(timer, 1000L, new AgeOutCache.Handler<Integer>() {
            public void expired(Integer key) {
                System.out.println(key + " expired");
            }
        });

        for(int i=1; i <= 5; i++)
            cache.add(i);

        System.out.println("cache:\n" + cache);
        assertEquals(5, cache.size());
        Util.sleep(1300);
        System.out.println("cache:\n" + cache);
        assertEquals(0, cache.size());
    }

    public void testRemoveAndExpiration() {
        cache=new AgeOutCache<Integer>(timer, 1000L);
        for(int i=1; i <= 5; i++)
            cache.add(i);

        System.out.println("cache:\n" + cache);
        Util.sleep(500);
        cache.remove(3);
        cache.remove(5);
        cache.remove(6); // not existent
        System.out.println("cache:\n" + cache);
        assertEquals(3, cache.size());
        Util.sleep(700);
        assertEquals(0, cache.size());
    }


    public void testGradualExpiration() {
        cache=new AgeOutCache<Integer>(timer, 1000L);
        for(int i=1; i <= 10; i++) {
            cache.add(i);
            System.out.print(".");
            Util.sleep(300);
        }
        System.out.println("\ncache:\n" + cache);
        int size=cache.size();
        assertTrue(size < 10 && size > 0);
    }



    public static Test suite() {
        TestSuite suite;
        suite=new TestSuite(AgeOutCacheTest.class);
        return (suite);
    }

    public static void main(String[] args) {
        String[] name={AgeOutCacheTest.class.getName()};
        junit.textui.TestRunner.main(name);
    }
}