package org.jgroups.protocols.pbcast;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Global;
import org.jgroups.Message;
import org.jgroups.annotations.LocalAddress;
import org.jgroups.annotations.MBean;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.Property;
import org.jgroups.conf.PropertyConverters;
import org.jgroups.stack.IpAddress;
import org.jgroups.util.Digest;
import org.jgroups.util.ShutdownRejectedExecutionHandler;
import org.jgroups.util.StateTransferResult;
import org.jgroups.util.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <code>STREAMING_STATE_TRANSFER</code>, as its name implies, allows a
 * streaming state transfer between two channel instances.
 * <p>
 * 
 * Major advantage of this approach is that transferring application state to a
 * joining member of a group does not entail loading of the complete application
 * state into memory. Application state, for example, might be located entirely
 * on some form of disk based storage. The default <code>STATE_TRANSFER</code>
 * requires this state to be loaded entirely into memory before being
 * transferred to a group member while <code>STREAMING_STATE_TRANSFER</code>
 * does not. Thus <code>STREAMING_STATE_TRANSFER</code> protocol is able to
 * transfer application state that is very large (>1Gb) without a likelihood of
 * such transfer resulting in OutOfMemoryException.
 * <p>
 * 
 * <code>STREAMING_STATE_TRANSFER</code> allows use of either default channel
 * transport or separate tcp sockets for state transfer. If firewalls are not a
 * concern then separate tcp sockets should be used as they offer faster state
 * transfer. Transport for state transfer is selected using
 * <code>use_default_transport<code> boolean property. 
 * <p>
 *  
 * 
 * Channel instance can be configured with either
 * <code>STREAMING_STATE_TRANSFER</code> or <code>STATE_TRANSFER</code> but not
 * both protocols at the same time.
 * 
 * <p>
 * 
 * In order to process streaming state transfer an application has to implement
 * <code>ExtendedMessageListener</code> if it is using channel in a push style
 * mode or it has to process <code>StreamingSetStateEvent</code> and
 * <code>StreamingGetStateEvent</code> if it is using channel in a pull style
 * mode.
 * 
 * 
 * @author Vladimir Blagojevic
 * @author Bela Ban
 * @see STATE_TRANSFER
 * @since 2.4
 */
@MBean(description = "State trasnfer protocol based on streaming state transfer")
public class STREAMING_STATE_TRANSFER_SOCKET extends StreamingStateTransfer {

    /*
     * ----------------------------------------------Properties -----------------------------------
     *
     */

    @LocalAddress
    @Property(description = "The interface (NIC) used to accept state requests. " +
            "The following special values are also recognized: GLOBAL, SITE_LOCAL, LINK_LOCAL and NON_LOOPBACK",
              systemProperty={Global.BIND_ADDR},
              defaultValueIPv4=Global.NON_LOOPBACK_ADDRESS, defaultValueIPv6=Global.NON_LOOPBACK_ADDRESS)
    protected InetAddress bind_addr;

    @Property(name="bind_interface", converter=PropertyConverters.BindInterface.class,
    		description="The interface (NIC) which should be used by this transport", dependsUpon="bind_addr")
    protected String bind_interface_str=null;

    @Property(description = "The port listening for state requests. Default value of 0 binds to any (ephemeral) port")
    protected int bind_port = 0;

    @Property(description = "Buffer size for state transfer. Default is 8192 bytes")
    protected int socket_buffer_size = 8 * 1024;

    @Property(description = "If use_default_transport=true, this is the input stream buffer size before a state producer is blocked")
    protected int buffer_queue_size = 81920;

    @Property(description = "If true default transport is used for state transfer rather than seperate TCP sockets. Default is false")
    boolean use_default_transport = false;


    /*
     * --------------------------------------------- Fields ---------------------------------------
     */


    /*
     * Runnable that listens for state requests and spawns threads to serve
     * those requests if socket transport is used
     */
    protected StateProviderThreadSpawner spawner;


    public STREAMING_STATE_TRANSFER_SOCKET() {
        super();
    }

    @ManagedAttribute
    public int getNumberOfStateRequests() {
        return num_state_reqs.get();
    }

    @ManagedAttribute
    public long getNumberOfStateBytesSent() {
        return num_bytes_sent.get();
    }

    @ManagedAttribute
    public double getAverageStateSize() {
        return avg_state_size;
    }

    public Vector<Integer> requiredDownServices() {
        Vector<Integer> retval = new Vector<Integer>();
        retval.addElement(new Integer(Event.GET_DIGEST));
        retval.addElement(new Integer(Event.OVERWRITE_DIGEST));
        return retval;
    }

    public void resetStats() {
        super.resetStats();
        num_state_reqs.set(0);
        num_bytes_sent.set(0);
        avg_state_size = 0;
    }


    public void start() throws Exception {
        Map<String, Object> map = new HashMap<String, Object>();
        map.put("state_transfer", Boolean.TRUE);
        map.put("protocol_class", getClass().getName());
        up_prot.up(new Event(Event.CONFIG, map));
        
        if (use_default_transport) {
            int size = buffer_queue_size / socket_buffer_size;
            // idiot proof it
            if (size <= 1) {
                size = 10;
            }
            if (log.isDebugEnabled())
                log.debug("buffer_queue_size=" + buffer_queue_size + ", socket_buffer_size="
                        + socket_buffer_size + ", creating queue of size " + size);
        }
    }

    public void stop() {
        super.stop();
        if (spawner != null)
            spawner.stop();
    }

    

    /*
     * --------------------------- Private Methods ------------------------------------------------
     */


    protected void modifyStateResponseHeader(StateHeader hdr) {
        if(spawner != null)
            hdr.bind_addr=spawner.getServerSocketAddress();
    }

    protected void respondToStateRequester(Address stateRequester, boolean open_barrier) {

        // setup socket plumbing if needed
        if (spawner == null && !use_default_transport) {
            spawner = new StateProviderThreadSpawner(createThreadPool(),
                                                     Util.createServerSocket(getSocketFactory(),
                                                                             Global.STREAMING_STATE_TRANSFER_SERVER_SOCK,
                                                                             bind_addr, bind_port));
            Thread t = getThreadFactory().newThread(spawner, "STREAMING_STATE_TRANSFER server socket acceptor");
            t.start();
        }

        Digest digest = isDigestNeeded() ? (Digest) down_prot.down(Event.GET_DIGEST_EVT) : null;

        Message state_rsp = new Message(stateRequester);
        StateHeader hdr = new StateHeader(StateHeader.STATE_RSP, use_default_transport?
          null : spawner.getServerSocketAddress(), digest);
        state_rsp.putHeader(this.id, hdr);

        if (log.isDebugEnabled())
            log.debug("Responding to state requester " + state_rsp.getDest() + " with address "
                    + (use_default_transport ? null : spawner.getServerSocketAddress()) + " and digest " + digest);
        down_prot.down(new Event(Event.MSG, state_rsp));
        if (stats)
            num_state_reqs.incrementAndGet();

        if (open_barrier)
            down_prot.down(new Event(Event.OPEN_BARRIER));
    }

    protected ThreadPoolExecutor createThreadPool() {
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(0, max_pool, pool_thread_keep_alive,
                                                               TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>());

        ThreadFactory factory = new ThreadFactory() {
            protected final AtomicInteger threadNumber = new AtomicInteger(1);
           
            public Thread newThread(final Runnable command) {
                return getThreadFactory().newThread(command, "STREAMING_STATE_TRANSFER-sender-" 
                      + threadNumber.getAndIncrement());
            }
        };
        threadPool.setRejectedExecutionHandler(new ShutdownRejectedExecutionHandler(threadPool.getRejectedExecutionHandler()));
        threadPool.setThreadFactory(factory);
        return threadPool;
    }

    protected Address determineCoordinator() {
        synchronized (members) {
            for (Address member : members) {
                if (!local_addr.equals(member))
                    return member;
            }
        }
        return null;
    }



    protected void handleStateReq(Address sender) {
        if (sender == null) {
            if (log.isErrorEnabled())
                log.error("sender of STATE_REQ is null; ignoring state transfer request");
            return;
        }

        if (isDigestNeeded())
            down_prot.down(new Event(Event.CLOSE_BARRIER)); // drain and block incoming msgs until the state has been returned

        try {
            respondToStateRequester(sender, isDigestNeeded());
        } catch (Throwable t) {
            if (log.isErrorEnabled())
                log.error("failed fetching state from application", t);
            if (isDigestNeeded())
                down_prot.down(new Event(Event.OPEN_BARRIER));
        }
    }

    protected void createStreamToRequester(Address requester) {
        // todo: create server socket, on accept() --> call getStateFromApplication()
    }

    protected void createStreamToProvider(Address provider, StateHeader hdr) {
    }

    void handleStateRsp(final StateHeader hdr, Address sender) {
        Digest tmp_digest = hdr.my_digest;
        if (isDigestNeeded()) {
            if (tmp_digest == null) {
                if (log.isWarnEnabled())
                    log.warn("digest received from " + sender + " is null, skipping setting digest !");
            } else {
                down_prot.down(new Event(Event.OVERWRITE_DIGEST, tmp_digest));
            }
        }

        connectToStateProvider(hdr, sender);
    }


    protected void handleEOF(Address sender) {
    }

    protected void handleException(Address sender, Throwable exception) {
    }

    protected void connectToStateProvider(StateHeader hdr, Address sender) {
        IpAddress address = hdr.bind_addr;
        InputStream bis = null;
        Socket socket = new Socket();
        try {
            socket.bind(new InetSocketAddress(bind_addr, 0));
            socket.setReceiveBufferSize(socket_buffer_size);
            if (log.isDebugEnabled())
                log.debug("Connecting to state provider " + address.getIpAddress() + ":" + address.getPort());
            
            Util.connect(socket, new InetSocketAddress(address.getIpAddress(), address.getPort()), 0);
            if (log.isDebugEnabled())
                log.debug("Connected to state provider, my socket is " + socket.getLocalAddress() + ":" + socket.getLocalPort());

            // write out our state_id and address
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(local_addr);
            bis = new BufferedInputStream(new StreamingInputStreamWrapper(socket),socket_buffer_size);
            up_prot.up(new Event(Event.STATE_TRANSFER_INPUTSTREAM, bis));
        } catch (IOException e) {
            if (log.isWarnEnabled())
                log.warn("State reader socket thread spawned abnormaly", e);

            handleException(sender, e);
        } finally {
            if (!socket.isConnected()) {
                if (log.isWarnEnabled())
                    log.warn("Could not connect to state provider. Closing socket...");
            }
            Util.close(bis);
            Util.close(socket);
        }
    }

    /*
     * ------------------------ End of Private Methods --------------------------------------------
     */

    protected class StateProviderThreadSpawner implements Runnable {
        protected final ExecutorService pool;
        protected final ServerSocket    serverSocket;
        protected final IpAddress       address;
        protected Thread                runner;
        protected volatile boolean      running = true;

        public StateProviderThreadSpawner(ExecutorService pool, ServerSocket stateServingSocket) {
            super();
            this.pool = pool;
            this.serverSocket = stateServingSocket;
            this.address = new IpAddress(STREAMING_STATE_TRANSFER_SOCKET.this.bind_addr, serverSocket.getLocalPort());
        }

        public void run() {
            runner = Thread.currentThread();
            while(running) {
                try {
                    if (log.isDebugEnabled())
                        log.debug("StateProviderThreadSpawner listening at " + getServerSocketAddress());

                    final Socket socket = serverSocket.accept();
                    pool.execute(new Runnable() {
                        public void run() {
                            if (log.isDebugEnabled())
                                log.debug("Accepted state transfer request from " + socket.getInetAddress() + ":" +
                                            socket.getPort() + "; handing off to thread pool");
                            new StateProviderHandler().process(socket);
                        }
                    });

                } catch (IOException e) {
                    if (log.isWarnEnabled()) {
                        // we get this exception when we close server socket
                        // exclude that case
                        if (!serverSocket.isClosed())
                            log.warn("Spawning socket from server socket finished abnormally", e);
                    }
                }
            }
        }

        public IpAddress getServerSocketAddress() {
            return address;
        }

        public void stop() {
            running = false;
            try {
                getSocketFactory().close(serverSocket);
            } catch (Exception ignored) {
            } finally {
                if (log.isDebugEnabled())
                    log.debug("Waiting for StateProviderThreadSpawner to die ... ");

                if (runner != null) {
                    try {
                        runner.join(Global.THREAD_SHUTDOWN_WAIT_TIME);
                    } catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (log.isDebugEnabled())
                    log.debug("Shutting the thread pool down... ");

                pool.shutdownNow();
                try {
                    pool.awaitTermination(Global.THREADPOOL_SHUTDOWN_WAIT_TIME, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
            if (log.isDebugEnabled())
                log.debug("Thread pool is shutdown. All pool threads are cleaned up.");
        }
    }

    protected class StateProviderHandler {
        public void process(Socket socket) {
            OutputStream bos = null;
            ObjectInputStream ois = null;
            try {
                int bufferSize = socket.getSendBufferSize();
                socket.setSendBufferSize(socket_buffer_size);
                if (log.isDebugEnabled())
                    log.debug("Running on " + Thread.currentThread()
                            + ". Accepted request for state transfer from "
                            + socket.getInetAddress() + ":" + socket.getPort()
                            + ", original buffer size was " + bufferSize + " and was reset to "
                            + socket.getSendBufferSize() + ", passing outputstream up... ");

                ois = new ObjectInputStream(socket.getInputStream());
                Address stateRequester = (Address) ois.readObject();
                bos = new BufferedOutputStream(new StreamingOutputStreamWrapper(socket),socket_buffer_size);
                up_prot.up(new Event(Event.STATE_TRANSFER_OUTPUTSTREAM, bos));
            } catch (IOException e) {
                if (log.isWarnEnabled()) {
                    log.warn("State writer socket thread spawned abnormaly", e);
                }
            } catch (ClassNotFoundException e) {
                // thrown by ois.readObject();  should never happen since String/Address are core classes
            } finally {
                if (!socket.isConnected()) {
                    if (log.isWarnEnabled())
                        log.warn("Could not receive connection from state receiver. Closing socket...");
                }
                Util.close(bos);
                Util.close(socket);
            }
        }
    }

    protected class StreamingInputStreamWrapper extends FilterInputStream {

        protected final Socket inputStreamOwner;

        protected final AtomicBoolean closed = new AtomicBoolean(false);

        public StreamingInputStreamWrapper(Socket inputStreamOwner) throws IOException {
            super(inputStreamOwner.getInputStream());
            this.inputStreamOwner = inputStreamOwner;
        }

        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("State reader is closing the socket ");
                }
                Util.close(inputStreamOwner);
                up(new Event(Event.STATE_TRANSFER_INPUTSTREAM_CLOSED, new StateTransferResult()));
            }
            super.close();
        }
    }

    protected class StreamingOutputStreamWrapper extends FilterOutputStream {
        protected final Socket outputStreamOwner;

        protected final AtomicBoolean closed = new AtomicBoolean(false);

        protected long bytesWrittenCounter = 0;

        public StreamingOutputStreamWrapper(Socket outputStreamOwner) throws IOException {
            super(outputStreamOwner.getOutputStream());
            this.outputStreamOwner = outputStreamOwner;
        }

        public void close() throws IOException {
            if (closed.compareAndSet(false, true)) {
                if (log.isDebugEnabled()) {
                    log.debug("State writer is closing the socket ");
                }
                Util.close(outputStreamOwner);
                if (stats)
                    avg_state_size = num_bytes_sent.addAndGet(bytesWrittenCounter) / num_state_reqs.doubleValue();
                super.close();
            }
        }

        public void write(byte[] b, int off, int len) throws IOException {            
            // Thanks Kornelius Elstner
            // https://jira.jboss.org/browse/JGRP-1223
            out.write(b, off, len);
            bytesWrittenCounter += len;
        }

        public void write(byte[] b) throws IOException {
            super.write(b);
            bytesWrittenCounter += b.length;
        }

        public void write(int b) throws IOException {
            super.write(b);
            bytesWrittenCounter += 1;
        }
    }



}
