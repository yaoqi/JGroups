package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * <code>STREAMING_STATE_TRANSFER_SOCKET</code> has the state provider create a server socket to which the state
 * requester connects and from which the latter reads the state.
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * @see STATE_TRANSFER
 * @since 3.0
 */
@MBean(description="State trasnfer protocol based on streaming state transfer")
public class STREAMING_STATE_TRANSFER_SOCKET extends StreamingStateTransfer {

    /*
     * ----------------------------------------------Properties -----------------------------------
     */
    @LocalAddress
    @Property(description="The interface (NIC) used to accept state requests. " +
      "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR},
              defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS)
    protected InetAddress bind_addr;

    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class,
              description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr")
    protected String bind_interface_str=null;

    @Property(description="The port listening for state requests. Default value of 0 binds to any (ephemeral) port")
    protected int bind_port=0;


    /*
    * --------------------------------------------- Fields ---------------------------------------
    */

    /**
     * Runnable that listens for state requests and spawns threads to serve those requests if socket transport is used
     */
    protected volatile StateProviderAcceptor spawner;


    public STREAMING_STATE_TRANSFER_SOCKET() {
        super();
    }


    public void stop() {
        super.stop();
        if(spawner != null)
            spawner.stop();
    }


    /*
    * --------------------------- Private Methods ------------------------------------------------
    */

    protected StateProviderAcceptor createAcceptor() {
        StateProviderAcceptor retval=new StateProviderAcceptor(thread_pool,
                                                               Util.createServerSocket(getSocketFactory(),
                                                                                       Global.STREAMING_STATE_TRANSFER_SERVER_SOCK,
                                                                                       bind_addr, bind_port));
        Thread t=getThreadFactory().newThread(retval, "STREAMING_STATE_TRANSFER server socket acceptor");
        t.start();
        return retval;
    }


    protected void modifyStateResponseHeader(StateHeader hdr) {
        if(spawner != null)
            hdr.bind_addr=spawner.getServerSocketAddress();
    }

    /*private void respondToStateRequester(Address stateRequester, boolean open_barrier) {

        // setup socket plumbing if needed
        if(spawner == null) {
            spawner=new StateProviderAcceptor(createThreadPool(),
                                                   Util.createServerSocket(getSocketFactory(),
                                                                           Global.STREAMING_STATE_TRANSFER_SERVER_SOCK,
                                                                           bind_addr, bind_port));
            Thread t=getThreadFactory().newThread(spawner, "STREAMING_STATE_TRANSFER server socket acceptor");
            t.start();
        }

        Digest digest=isDigestNeeded()? (Digest)down_prot.down(Event.GET_DIGEST_EVT) : null;

        Message state_rsp=new Message(stateRequester);
        StateHeader hdr=new StateHeader(StateHeader.STATE_RSP, spawner.getServerSocketAddress(), digest);
        state_rsp.putHeader(this.id, hdr);

        if(log.isDebugEnabled())
            log.debug("Responding to state requester " + state_rsp.getDest() + " with address "
                        + spawner.getServerSocketAddress() + " and digest " + digest);
        down_prot.down(new Event(Event.MSG, state_rsp));
        if(stats)
            num_state_reqs.incrementAndGet();

        if(open_barrier)
            down_prot.down(new Event(Event.OPEN_BARRIER));
    }*/


    protected void createStreamToRequester(Address requester) {
        ;
    }

    protected void createStreamToProvider(Address provider, StateHeader hdr) {
        IpAddress address=hdr.bind_addr;
        InputStream bis=null;
        Socket socket=new Socket();
        try {
            socket.bind(new InetSocketAddress(bind_addr, 0));
            socket.setReceiveBufferSize(buffer_size);
            Util.connect(socket, new InetSocketAddress(address.getIpAddress(), address.getPort()), 0);
            if(log.isDebugEnabled())
                log.debug("connected to state provider " + address.getIpAddress() + ":" + address.getPort());

            // write out our state_id and address
            ObjectOutputStream out=new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(local_addr);
            bis=new BufferedInputStream(new StreamingInputStreamWrapper(socket), buffer_size);
            setStateInApplication(provider, bis, hdr.getDigest());
        }
        catch(IOException e) {
            if(log.isWarnEnabled())
                log.warn("state reader socket thread spawned abnormally", e);
            handleException(provider, e);
        }
        finally {
            if(!socket.isConnected()) {
                if(log.isWarnEnabled())
                    log.warn("could not connect to state provider. Closing socket...");
            }
            Util.close(bis);
            Util.close(socket);
        }
    }


    protected void handleStateReq(Address requester) {
        if(spawner == null || !spawner.isRunning())
            spawner=createAcceptor();
        super.handleStateReq(requester);
    }

    protected void handleEOF(Address sender) {
        openBarrierAndResumeStable();
        up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
    }

    protected void handleException(Address sender, Throwable exception) {
        openBarrierAndResumeStable();
        Exception ex=new Exception("state provider " + state_provider + " raised exception", exception);
        up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult(ex)));
    }

    /*protected void connectToStateProvider(StateHeader hdr, Address sender) {
        IpAddress address=hdr.bind_addr;
        InputStream bis=null;
        Socket socket=new Socket();
        try {
            socket.bind(new InetSocketAddress(bind_addr, 0));
            socket.setReceiveBufferSize(buffer_size);
            if(log.isDebugEnabled())
                log.debug("Connecting to state provider " + address.getIpAddress() + ":" + address.getPort());

            Util.connect(socket, new InetSocketAddress(address.getIpAddress(), address.getPort()), 0);
            if(log.isDebugEnabled())
                log.debug("Connected to state provider, my socket is " + socket.getLocalAddress() + ":" + socket.getLocalPort());

            // write out our state_id and address
            ObjectOutputStream out=new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(local_addr);
            bis=new BufferedInputStream(new StreamingInputStreamWrapper(socket), buffer_size);
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, bis));
        }
        catch(IOException e) {
            if(log.isWarnEnabled())
                log.warn("State reader socket thread spawned abnormaly", e);

            handleException(sender, e);
        }
        finally {
            if(!socket.isConnected()) {
                if(log.isWarnEnabled())
                    log.warn("Could not connect to state provider. Closing socket...");
            }
            Util.close(bis);
            Util.close(socket);
        }
    }*/

    /*
     * ------------------------ End of Private Methods --------------------------------------------
     */

    protected class StateProviderAcceptor implements Runnable {
        protected final ExecutorService pool;
        protected final ServerSocket serverSocket;
        protected final IpAddress address;
        protected Thread runner;
        protected volatile boolean running=true;

        public StateProviderAcceptor(ExecutorService pool, ServerSocket stateServingSocket) {
            super();
            this.pool=pool;
            this.serverSocket=stateServingSocket;
            this.address=new IpAddress(STREAMING_STATE_TRANSFER_SOCKET.this.bind_addr, serverSocket.getLocalPort());
        }

        public void run() {
            runner=Thread.currentThread();
            if(log.isDebugEnabled())
                log.debug("StateProviderAcceptor listening at " + getServerSocketAddress());
            while(running) {
                try {
                    final Socket socket=serverSocket.accept();
                    pool.execute(new Runnable() {
                        public void run() {
                            process(socket);
                        }
                    });

                }
                catch(Throwable e) {
                    if(serverSocket.isClosed())
                        running=false;
                }
            }
        }

        protected void process(Socket socket) {
            OutputStream output=null;
            ObjectInputStream ois=null;
            try {
                socket.setSendBufferSize(buffer_size);
                if(log.isDebugEnabled())
                    log.debug("accepted request for state transfer from " + socket.getInetAddress() + ":" + socket.getPort());

                ois=new ObjectInputStream(socket.getInputStream());
                Address stateRequester=(Address)ois.readObject();
                output=new BufferedOutputStream(socket.getOutputStream(), buffer_size);
                getStateFromApplication(stateRequester, output, false);
            }
            catch(Throwable e) {
                if(log.isWarnEnabled())
                    log.warn("failed connecting to state provider", e);
                Util.close(output);
                Util.close(socket);
            }
        }

        public IpAddress getServerSocketAddress() {
            return address;
        }

        public boolean isRunning() {
            return running;
        }

        public void stop() {
            running=false;
            try {
                getSocketFactory().close(serverSocket);
            }
            catch(Exception ignored) {
            }
        }
    }

   

    protected class StreamingInputStreamWrapper extends FilterInputStream {

        protected final Socket inputStreamOwner;

        protected final AtomicBoolean closed=new AtomicBoolean(false);

        public StreamingInputStreamWrapper(Socket inputStreamOwner) throws IOException {
            super(inputStreamOwner.getInputStream());
            this.inputStreamOwner=inputStreamOwner;
        }

        public void close() throws IOException {
            if(closed.compareAndSet(false, true)) {
                if(log.isDebugEnabled()) {
                    log.debug("state reader is closing the socket ");
                }
                Util.close(inputStreamOwner);
                up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
            }
            super.close();
        }
    }

    /*protected class StreamingOutputStreamWrapper extends FilterOutputStream {
        protected final Socket outputStreamOwner;

        protected final AtomicBoolean closed=new AtomicBoolean(false);

        protected long bytesWrittenCounter=0;

        public StreamingOutputStreamWrapper(Socket outputStreamOwner) throws IOException {
            super(outputStreamOwner.getOutputStream());
            this.outputStreamOwner=outputStreamOwner;
        }

        public void close() throws IOException {
            if(closed.compareAndSet(false, true)) {
                if(log.isDebugEnabled()) {
                    log.debug("State writer is closing the socket ");
                }
                Util.close(outputStreamOwner);
                if(stats)
                    avg_state_size=num_bytes_sent.addAndGet(bytesWrittenCounter) / num_state_reqs.doubleValue();
                super.close();
            }
        }

        public void write(byte[] b, int off, int len) throws IOException {
            // Thanks Kornelius Elstner
            // https://jira.jboss.org/browse/JGRP-1223
            out.write(b, off, len);
            bytesWrittenCounter+=len;
        }

        public void write(byte[] b) throws IOException {
            super.write(b);
            bytesWrittenCounter+=b.length;
        }

        public void write(int b) throws IOException {
            super.write(b);
            bytesWrittenCounter+=1;
        }
    }*/


}
