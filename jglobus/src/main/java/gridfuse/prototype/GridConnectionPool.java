package gridfuse.prototype;

import java.io.IOException;
import org.globus.ftp.GridFTPClient;
import org.globus.ftp.GridFTPSession;
import org.globus.ftp.examples.LocalCredentialHelper;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

/**
 * Class contains functionality for generating and managing a pool
 * of GridFTP connections.
 */
public class GridConnectionPool {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );
    private BlockingQueue<GridFTPClient> availableConnections;
    GSSCredential cred;
    final String remoteHost;
    final int remotePort;
    final String remoteMode;
    final int MAX_POOL_SIZE;
    final String conf_dir;

    /**
     * @param host, server hostname.
     * @param port, server port.
     * @param maxPoolSize, total number of connections to keep in the pool.
     * @param mode, whether the pool should contain ASCII or Binary connections.
     *
     * @throws IOException when credentials are not found.
     */
    public GridConnectionPool(String host, int port, int maxPoolSize, String mode, String grid_fuse_conf) throws IOException {
        if (!mode.equals("ASCII") && !mode.equals("Binary")) {
            throw new IllegalArgumentException("Server mode is neither ASCII nor Binary");
        }
        remoteHost = host;
        remotePort = port;
        remoteMode = mode;
        MAX_POOL_SIZE = maxPoolSize;
        conf_dir = grid_fuse_conf;
        availableConnections = new ArrayBlockingQueue<GridFTPClient>(MAX_POOL_SIZE);
        initialiseConnectionPool();
    }

    /**
     * Creates a connection in the pool and then starts a thread to create
     * the remainder, upto max pool size.
     *
     * @throws IOException when credentials are not found.
     */
    private void initialiseConnectionPool() throws IOException {
        try {
            cred = new GridFuseCredHelper().getCredentialFrom(conf_dir);
        } catch (GSSException gssE) { throw new IOException("Was a GSSException. Change this.", gssE); }
        //Create a connection in the main thread, so there will definitely
        //be one when the first call comes in.
        if (!createNewConnectionForPool()) {
            LOGGER.severe("Failed to create first connection for pool");
            throw new IOException("Failed to connect to remote store");
        }
        //Then start a new thread to fill in the rest of the pool.
        ExecutorService exec = Executors.newSingleThreadExecutor();
        exec.submit(() -> {
            boolean spaceInPool = true;
            while(spaceInPool) {
                spaceInPool = createNewConnectionForPool();
            }
        });
        exec.shutdown();
    }

    /**
     * Creates a single connection and adds it to the connection pool.
     * Provided the pool is not already full.
     *
     * @return boolean, true when new connection created and added to pool,
     *                  false when pool is full
     */
    private boolean createNewConnectionForPool() {
        GridFTPClient conn = null;
        boolean connectionAdded = false;
        try {
            LOGGER.info("Remaining credential lifetime: " + cred.getRemainingLifetime()/60 + "mins");
        } catch (GSSException gssE) {
            LOGGER.log(Level.SEVERE,"Can't get credential lifetime. Credential probably expired.",gssE);
        }
        try {
            conn = new GridFTPClient(remoteHost, remotePort);
            conn.authenticate(cred);
            //ASCII for sending requests like mlsd, delete, rename, etc...
            if (remoteMode == "ASCII") {
                conn.setType(GridFTPSession.TYPE_ASCII);
                conn.setMode(GridFTPSession.MODE_STREAM);
            }
            //Binary for transfering files
            else if (remoteMode == "Binary") {
                conn.setType(GridFTPSession.TYPE_IMAGE);
                conn.setMode(GridFTPSession.MODE_EBLOCK);
            }
            else {
                throw new IllegalArgumentException("Server mode is neither ASCII nor Binary");
            }
            //Tell GridFTP to add encryption
            conn.setDataChannelProtection(GridFTPSession.PROTECTION_PRIVATE);
            //Try to add this connection to the pool
            connectionAdded = availableConnections.offer(conn);
            if (connectionAdded) {
                LOGGER.finest("Connection added to pool");
            } else {
                LOGGER.finest("Warning connection not added to pool");
            }
        } catch (ServerException sE) {
            LOGGER.log(Level.SEVERE, "ServerException in createNewConnectionForPool()", sE);
        } catch (IOException ioE) {
            LOGGER.log(Level.SEVERE, "IOException in createNewConnectionForPool()", ioE);
        } finally {
            return connectionAdded;
        }
    }

    /**
     * Removes a connection from the pool and returns it.
     * If the pool is empty, this will wait for 120 seconds
     * for a connection to appear.
     *
     * @return GridFTPClient connection
     */
    public GridFTPClient getConnectionFromPool() {
        GridFTPClient conn = null;
        LOGGER.fine("available connections: " + availableConnections.size());
        try {
            conn = (GridFTPClient) availableConnections.poll(120,TimeUnit.SECONDS);
        } catch (InterruptedException iE) {
            LOGGER.log(Level.SEVERE,"taking connection from pool interrupted",iE);
            return conn;
        }
        if (conn != null) {
            if (remoteMode == "ASCII") {
                //GridFTP requires us to set these between
                //each operation in ASCII mode.
                try {
                    conn.setPassive();
                    conn.setLocalActive();
                } catch (IOException|ClientException|ServerException e) {
                    LOGGER.log(Level.SEVERE, "Cannot set connection to Passive/LocalActive", e);
                    return null;
                }
            }
        }
        else {
            LOGGER.severe("\nUnable to acquire connection, returning null\n");
        }
        return conn;
    }

    /**
     * Takes given connection and closes it,
     * replaces it with a new connection in the pool.
     *
     * @param conn The connection to be returned.
     */
    public void returnConnectionToPool(GridFTPClient conn) {
        try {
            conn.close();
        } catch (IOException ioE) {
            LOGGER.log(Level.FINEST, "Error closing old connection.", ioE);
        } catch (ServerException sE) {
            LOGGER.log(Level.FINEST, "Error closing old connection.", sE);
        }
        createNewConnectionForPool();
    }

    /**
     * Performs a NOOP request to all connections in the pool.
     */
    public void noopAll() {
        Iterator<GridFTPClient> connIter = availableConnections.iterator();
        while(connIter.hasNext()) {
            try {
                connIter.next().quote("NOOP");
            } catch (IOException|ServerException E) {
                LOGGER.log(Level.SEVERE,"Error performing NOOP command",E);
            }
        }
    }
}

