package org.jgroups.jmx.protocols;

import org.jgroups.jmx.ProtocolMBean;

/**
 * @author Bela Ban
 * @version $Id: UNICASTMBean.java,v 1.8.6.1 2009/04/15 07:18:45 belaban Exp $
 */
public interface UNICASTMBean extends ProtocolMBean {
    String getLocalAddress();
    String getMembers();
    String printConnections();
    long getMessagesSent();
    long getMessagesReceived();
    long getBytesSent();
    long getBytesReceived();
    long getAcksSent();
    long getAcksReceived();
    long getXmitRequestsReceived();
    int getNumUnackedMessages();
    String getUnackedMessages();
    int getNumberOfMessagesInReceiveWindows();
    long getMaxRetransmitTime();
    void setMaxRetransmitTime(long max_retransmit_time);
    int getAgeOutCacheSize();
    String printAgeOutCache();
}
