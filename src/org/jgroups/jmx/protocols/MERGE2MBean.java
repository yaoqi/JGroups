package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: MERGE2MBean.java,v 1.1.16.1 2009/03/31 14:51:53 belaban Exp $
 */
public interface MERGE2MBean extends ProtocolMBean {
    long getMinInterval();
    void setMinInterval(long i);
    long getMaxInterval();
    void setMaxInterval(long l);
    boolean getSuspended();
    void suspendMergeTask();
    void resumeMergeTask();
    void sendMergeSolicitation();
}
