package org.jgroups.util;

import org.jgroups.Address;
import org.jgroups.annotations.GuardedBy;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.TimeUnit;

/** Similar to AckCollector, but collects responses, not just acks. Note that, once done, the object cannot be
 * reused, but a new instance should be created. Null is not a valid key.
 * @author Bela Ban
 * @version $Id: ResponseCollector.java,v 1.1.2.2 2009/04/20 13:00:17 belaban Exp $
 */
public class ResponseCollector<T> {
    @GuardedBy("lock")
    private final Map<Address,T> responses;
    private final Lock lock=new ReentrantLock(false);
    private final Condition cond=lock.newCondition();
    private volatile boolean stopped=false;


    /**
     *
     * @param members List of members from which we expect responses
     */
    public ResponseCollector(Collection<Address> members) {
        if(members == null)
            throw new IllegalArgumentException("members cannot be null");
        responses=new HashMap<Address,T>(members.size());
        for(Address mbr: members)
            responses.put(mbr, null);
    }

    public void add(Address member, T data) {
        if(member == null || stopped)
            return;
        lock.lock();
        try {
            if(responses.containsKey(member)) {
                responses.put(member, data);
                cond.signalAll();
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void suspect(Address member) {
        if(member == null || stopped)
            return;
        lock.lock();
        try {
            if(responses.remove(member) != null)
                cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public boolean hasAllResponses() {
        if(stopped) return false;
        lock.lock();
        try {
            for(Map.Entry<Address,T> entry: responses.entrySet()) {
                if(entry.getValue() == null)
                    return false;
            }
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    public Map<Address,T> getResults() {
        return Collections.unmodifiableMap(responses);
    }

    public int size() {
        lock.lock();
        try {
            return responses.size();
        }
        finally {
            lock.unlock();
        }
    }


    /**
     * Waits until all responses have been received, or until a timeout has elapsed.
     * @param timeout Number of milliseconds to wait max. This value needs to be greater than 0, or else
     * it will be adjusted to 2000
     * @return boolean True if all responses have been received within timeout ms, else false (e.g. if interrupted)
     */
    public boolean waitForAllResponses(long timeout) {
        if(timeout <= 0)
            timeout=2000L;
        long end_time=System.currentTimeMillis() + timeout;
        long wait_time;

        lock.lock();
        try {
            while(!hasAllResponses() && !stopped) {
                wait_time=end_time - System.currentTimeMillis();
                if(wait_time <= 0)
                    return false;
                try {
                    cond.await(wait_time, TimeUnit.MILLISECONDS);
                }
                catch(InterruptedException e) {
                    Thread.currentThread().interrupt(); // set interrupt flag again
                    return false;
                }
            }
            return !stopped;
        }
        finally {
            lock.unlock();
        }
    }

    public void stop() {
        lock.lock();
        try {
            stopped=true;
            responses.clear();
            cond.signalAll();
        }
        finally {
            lock.unlock();
        }
    }

    public String toString() {
        StringBuilder sb=new StringBuilder();
        sb.append(responses).append(", complete=").append(hasAllResponses());
        return sb.toString();
    }
}
