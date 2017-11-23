package gridfuse.prototype;

import java.io.IOException;
import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.TimeZone;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Vector;
import java.util.ArrayList;

import org.globus.ftp.GridFTPClient;
import org.globus.ftp.exception.ClientException;
import org.globus.ftp.exception.ServerException;
import org.globus.ftp.MlsxEntry;

/**
 * Contains all the methods required to perform
 * remote filesystem operations.
 */
public class GridOpsGridFTP implements GridOps {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );

    //Connection pools
    protected GridConnectionPool mainstoreASCIIpool;
    protected GridConnectionPool mainstoreBinaryGetpool;
    protected GridConnectionPool mainstoreBinarySendpool;
    protected GridConnectionPool tnodeBinaryGetpool;
    protected GridConnectionPool tnodeBinarySendpool;
    protected String localrootdir;
    protected String mainstorerootdir;
    protected String cacherootdir;
    protected int maxPoolSize;
    GridFuseDirTree dirTree;
    
    /**
     * @param GridFuseProps, parsed configuration properties for GridFTP
     * @param dirTree, directory tree object
     *
     * @throws IOException when myproxy certificate is missing.
     */
    public GridOpsGridFTP(HashMap<String,String> GridFuseProps, GridFuseDirTree dirTree) throws IOException {
        this.dirTree = dirTree;
        String mainstorehost    = GridFuseProps.get("MainStoreHost");
        int mainstoreport       = Integer.parseInt(GridFuseProps.get("MainStorePort"));
        String tnodehost        = GridFuseProps.get("TransferNodeHost");
        int tnodeport           = Integer.parseInt(GridFuseProps.get("TransferNodePort"));
        maxPoolSize             = Integer.parseInt(GridFuseProps.get("maxPoolSize"));
        localrootdir            = GridFuseProps.get("localrootdir");
        cacherootdir            = GridFuseProps.get("cacherootdir");
        mainstorerootdir        = GridFuseProps.get("mainstorerootdir");
        String grid_fuse_conf   = GridFuseProps.get("GRID_FUSE_CONF");

        try {
            mainstoreASCIIpool      = new GridConnectionPool(mainstorehost,mainstoreport,maxPoolSize,"ASCII",grid_fuse_conf);
            mainstoreBinaryGetpool  = new GridConnectionPool(mainstorehost,mainstoreport,maxPoolSize,"Binary",grid_fuse_conf);
            mainstoreBinarySendpool = new GridConnectionPool(mainstorehost,mainstoreport,maxPoolSize,"Binary",grid_fuse_conf);
            tnodeBinaryGetpool      = new GridConnectionPool(tnodehost,tnodeport,maxPoolSize,"Binary",grid_fuse_conf);
            tnodeBinarySendpool     = new GridConnectionPool(tnodehost,tnodeport,maxPoolSize,"Binary",grid_fuse_conf);
        } catch (IOException ioE) {
            LOGGER.info("Remember to get myproxy certificate.");
            throw new IOException(ioE);
        }
    }

    /**
     * Transfers a file to or from remote server, depending on direction parameter.
     *
     * @param path, directory path
     * @param direction, FileStat.CACHE_BEHIND = get file from remote server.
     *                  FileStat.CACHE_AHEAD = send file to remote server.
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int fileTransfer(String path, int direction) {
        GridFTPClient mainstore;
        GridFTPClient tnode;
        if (direction == FileStat.CACHE_BEHIND) {
            mainstore = mainstoreBinaryGetpool.getConnectionFromPool();
            tnode = tnodeBinaryGetpool.getConnectionFromPool();
        }
        else if (direction == FileStat.CACHE_AHEAD) {
            mainstore = mainstoreBinarySendpool.getConnectionFromPool();
            tnode = tnodeBinarySendpool.getConnectionFromPool();
        }
        else {
            LOGGER.severe("Bad direction, trying to transfer "+path+" with direction: "+direction);
            return -1;
        }
        if (mainstore == null) {
            LOGGER.severe("\n\nMainstore connection is null.\n\n");
            return -1;
        }
        if (tnode == null) {
            LOGGER.severe("\n\nTransfer Node connection is null.\n\n");
            return -1;
        }
        try {
            if (direction == FileStat.CACHE_AHEAD) {
                LOGGER.fine("Writing "+path+" back to mainstore");
                //Initiate third party transfer
                tnode.transfer(cacherootdir+path,mainstore,mainstorerootdir+path,false,null);
            }
            else if (direction == FileStat.CACHE_BEHIND) {
                LOGGER.fine("Caching "+path+" from mainstore");
                File file = new File(localrootdir+path);
                file.getParentFile().mkdirs();
                //Initiate third party transfer
                mainstore.transfer(mainstorerootdir+path,tnode,cacherootdir+path,false,null);
            }
        } catch (IOException ioE) {
            LOGGER.log(Level.SEVERE,"IOException in fileTransfer(): ", ioE);
            return -2;
        } catch (ClientException cE) {
            LOGGER.log(Level.SEVERE,"Client exception in fileTransfer(): ", cE);
            return -1;
        } catch (ServerException sE) {
            if (sE.toString().contains("No such file or directory")) {
                return -2;
            }
            LOGGER.log(Level.SEVERE,"Server exception in fileTransfer(): ", sE);
            return -1;
        }
        finally {
            if (direction == FileStat.CACHE_BEHIND) {
                mainstoreBinaryGetpool.returnConnectionToPool(mainstore);
                tnodeBinaryGetpool.returnConnectionToPool(tnode);
            }
            else if (direction == FileStat.CACHE_AHEAD) {
                mainstoreBinarySendpool.returnConnectionToPool(mainstore);
                tnodeBinarySendpool.returnConnectionToPool(tnode);
            }
        }
        return 0;
    }

    /**
     * Transfers files to or from remote server, depending on direction parameter.
     *
     * @param path, directory path
     * @param direction, FileStat.CACHE_BEHIND = get files from remote server.
     *                  FileStat.CACHE_AHEAD = send files to remote server.
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int multipleFileTransfer(String[] paths, int direction) {
        GridFTPClient mainstore;
        GridFTPClient tnode;
        if (direction == FileStat.CACHE_BEHIND) {
            mainstore = mainstoreBinaryGetpool.getConnectionFromPool();
            tnode = tnodeBinaryGetpool.getConnectionFromPool();
        }
        else if (direction == FileStat.CACHE_AHEAD) {
            mainstore = mainstoreBinarySendpool.getConnectionFromPool();
            tnode = tnodeBinarySendpool.getConnectionFromPool();
        }
        else {
            LOGGER.severe("Bad direction, trying to transfer a bunch of files with direction: "+direction);
            return -1;
        }
        if (mainstore == null) {
            LOGGER.severe("\n\nMainstore connection is null.\n\n");
            return -1;
        }
        if (tnode == null) {
            LOGGER.severe("\n\nTransfer Node connection is null.\n\n");
            return -1;
        }
        ArrayList<String> fullCachePaths = new ArrayList<String>();
        ArrayList<String> fullMainstorePaths = new ArrayList<String>();
        for(String path : paths) {
            if ( path != null ) {
                fullCachePaths.add(cacherootdir+path);
                fullMainstorePaths.add(mainstorerootdir+path);
            }
        }
        try {
            if (direction == FileStat.CACHE_AHEAD) {
                LOGGER.fine("Writing a bunch of files back to mainstore");
                /* TODO: These might be what we need to perform striped transfers, server will also need to be setup for it
                tnode.setOptions(new RetrieveOptions(5));
                HostPortList hpl = mainstore.setStripedPassive();
                tnode.setStripedActive(hpl);
                */
                //Initiate third party transfer
                tnode.extendedMultipleTransfer(fullCachePaths.toArray(new String[0]),mainstore,fullMainstorePaths.toArray(new String[0]),null,null);
            }
            else if (direction == FileStat.CACHE_BEHIND) {
                LOGGER.fine("Caching a bunch of files from mainstore");
                for(String path : paths) {
                    if ( path != null ) {
                        File file = new File(localrootdir+path);
                        file.getParentFile().mkdirs();
                    }
                }
                /* TODO: These might be what we need to perform striped transfers, server will also need to be setup for it
                mainstore.setOptions(new RetrieveOptions(5));
                HostPortList hpl = tnode.setStripedPassive();
                mainstore.setStripedActive(hpl);
                */
                //Initiate third party transfer
                mainstore.extendedMultipleTransfer(fullMainstorePaths.toArray(new String[0]),tnode,fullCachePaths.toArray(new String[0]),null,null);
            }
        } catch (IOException ioE) {
            LOGGER.log(Level.SEVERE,"IOException in fileTransfer(): ", ioE);
            return -2;
        } catch (ClientException cE) {
            LOGGER.log(Level.SEVERE,"Client exception in fileTransfer(): ", cE);
            return -1;
        } catch (ServerException sE) {
            if (sE.toString().contains("No such file or directory")) {
                return -2;
            }
            LOGGER.log(Level.SEVERE,"Server exception in fileTransfer(): ", sE);
            return -1;
        }
        finally {
            if (direction == FileStat.CACHE_BEHIND) {
                mainstoreBinaryGetpool.returnConnectionToPool(mainstore);
                tnodeBinaryGetpool.returnConnectionToPool(tnode);
            }
            else if (direction == FileStat.CACHE_AHEAD) {
                mainstoreBinarySendpool.returnConnectionToPool(mainstore);
                tnodeBinarySendpool.returnConnectionToPool(tnode);
            }
        }
        return 0;
    }

    /**
     * Creates a remote directory.
     *
     * @param path, directory path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int mkdir(String path) {
        GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
        try {
            mainstore.makeDir(mainstorerootdir+path);
        }
        catch (ServerException se) {
            LOGGER.log(Level.SEVERE,"Server exception in mkdir(): ", se);
            return -1;
        }
        catch (IOException ioE) {
            LOGGER.log(Level.SEVERE,"IOException in mkdir(): ", ioE);
            return -2;
        }
        finally {
            mainstoreASCIIpool.returnConnectionToPool(mainstore);
        }
        return 0;
    }

    /**
     * Deletes remote file.
     *
     * @param path, file path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int unlink(String path) {
        GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
        try {
            mainstore.deleteFile(mainstorerootdir+path);
        }
        catch (ServerException sE) {
            if (sE.toString().contains("No such file or directory")) {
                return -2;
            }
            LOGGER.log(Level.SEVERE,"ServerException in unlink(): ", sE);
            return -2;
        }
        catch (IOException ioE) {
            LOGGER.fine("IOException in unlink(): " + ioE);
            return -2;
        }
        finally {
            mainstoreASCIIpool.returnConnectionToPool(mainstore);
        }
        return 0;
    }

    /**
     * Deletes remote directory.
     *
     * @param path, directory path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int rmdir(String path) {
        GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
        try {
            mainstore.deleteDir(mainstorerootdir+path);
        }
        catch (ServerException sE) {
            if (sE.toString().contains("Directory not empty")) {
                return -39;
            }
            LOGGER.log(Level.SEVERE,"Server exception in rmdir(): ", sE);
            return -1;
        }
        catch (IOException ioE) {
            LOGGER.fine("IOException in rmdir(): " + ioE);
            return -2;
        }
        finally {
            mainstoreASCIIpool.returnConnectionToPool(mainstore);
        }
        return 0;
    }

    /**
     * Performs rename operation on remote file.
     *
     * @param path, old file path
     * @param newpath, new file path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int rename(String path, String newpath) {
        //Move and/or rename file on server
        GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
        try {
            mainstore.rename(mainstorerootdir+path,mainstorerootdir+newpath);
        }
        catch (ServerException se2) {
            LOGGER.log(Level.SEVERE,"ServerException in rename(): ", se2);
            return -2;
        }
        catch (IOException ioE) {
            LOGGER.fine("IOException in rename(): " + ioE);
            return -2;
        }
        finally {
            mainstoreASCIIpool.returnConnectionToPool(mainstore);
        }
        return 0;
    }

    /**
     * Changes the modification time of a file on the remote server.
     *
     * @param path, path to file to be changed
     * @param mtime, new modification time to be set, in microseconds.
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int changeMTime(String path, long mtime) {
        GregorianCalendar cal = new GregorianCalendar(TimeZone.getTimeZone("GMT"));
        cal.setTimeInMillis(mtime*1000);

        GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
        try {
            mainstore.changeModificationTime(cal.get(Calendar.YEAR),
                                            cal.get(Calendar.MONTH)+1,
                                            cal.get(Calendar.DAY_OF_MONTH),
                                            cal.get(Calendar.HOUR_OF_DAY),
                                            cal.get(Calendar.MINUTE),
                                            cal.get(Calendar.SECOND),
                                            mainstorerootdir+path);
        } catch (IOException ioE) {
            LOGGER.fine("IOException in changeModificationTime(): " + ioE);
            return -2;
        }
        catch (ServerException sE) {
            if (sE.toString().contains("Operation not permitted")) {
                return -1;
            }
            else {
                LOGGER.log(Level.SEVERE,"ServerException in changeModificationTime(): ", sE);
                return -1;
            }
        }
        finally {
            mainstoreASCIIpool.returnConnectionToPool(mainstore);
        }
        return 0;
    }

    /**
     * Searches through remote directory (path) and adds the
     * results to dirTree
     *
     * @param path the path to the directory you want to list
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int mlsd(String path) {
        Vector dir_listing = null;
        GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
        try {
            //If the file/directory doesn't exist on the server move up one.
            if ( !mainstore.exists(mainstorerootdir+path) ) {
                LOGGER.fine(path+" doesn't exist");
                path = path.substring(0,path.lastIndexOf("/"));
            }
            dir_listing = mainstore.mlsd(mainstorerootdir+path);
        } catch (ServerException sE) {
            if (sE.toString().contains("Permission denied")) {
                return -13;
            }
            else if (sE.toString().contains("Not a directory")) {
                return -20;
            }
            else {
                LOGGER.log(Level.SEVERE,"ServerException doing mlsd on "+path, sE);
                return -1;
            }
        } catch (ClientException cE) {
            LOGGER.log(Level.SEVERE,"ClientException doing mlsd on "+path, cE);
            return -1;
        } catch (IOException ioE) {
            LOGGER.log(Level.SEVERE,"IOException doing mlsd on "+path, ioE);
            return -2;
        }
        finally {
            mainstoreASCIIpool.returnConnectionToPool(mainstore);
        }

        //Loop through files received from gFTP
        while (!dir_listing.isEmpty()) {
            MlsxEntry f = (MlsxEntry) dir_listing.remove(0);
            DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmmss");
            dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
            long mtime;
            try { mtime = dfm.parse(f.get(MlsxEntry.MODIFY)).getTime()/1000;
            } catch (ParseException pE) { mtime = 0L; }

            //Create file entry
            FileStat fileMeta = new FileStat(f.getFileName().toString(),
                                        f.get(MlsxEntry.TYPE),
                                        f.get(MlsxEntry.UNIX_MODE),
                                        f.get(MlsxEntry.SIZE),
                                        mtime,
                                        f.get(MlsxEntry.UNIX_OWNER),
                                        f.get(MlsxEntry.UNIX_GROUP),
                                        FileStat.CACHE_BEHIND);
            //Add to dirTree
            dirTree.add_node(path,fileMeta);
        }
        return 0;
    }

    /**
     * Recursively searches through remote directory (path) and adds the
     * results to dirTree
     *
     * @param path the path to the top level directory you want to list from.
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int mlsr(String path) {
        //writer takes each MlsxEntry returned by MLSR,
        //parses it and adds it to the dirTree we pass here.
        GridFuseMlsxEntryWriter writer = new GridFuseMlsxEntryWriter(dirTree);
        //Check through entire directory tree of mainstore
        //and add contents to dirTree.
        try {
            GridFTPClient mainstore = mainstoreASCIIpool.getConnectionFromPool();
            if (mainstore != null) {
                mainstore.mlsr(mainstorerootdir+path,writer);
                mainstoreASCIIpool.returnConnectionToPool(mainstore);
            }
            else {
                return -1;
            }
        } catch (ServerException sE) {
            LOGGER.log(Level.SEVERE,"CacheAll MLSR ServerException: ", sE);
            return -1;
        }
        catch (ClientException cE) {
            LOGGER.log(Level.SEVERE,"CacheAll MLSR ClientException: ", cE);
            return -1;
        }
        catch (IOException ioE) {
            LOGGER.fine("CacheAll MLSR IOException: " + ioE);
            return -2;
        }
        return 0;
    }

    /**
     * Performs a NOOP request to all connections in each pool.
     */
    public void noop() {
        mainstoreASCIIpool.noopAll();
        mainstoreBinaryGetpool.noopAll();
        mainstoreBinarySendpool.noopAll();
        tnodeBinaryGetpool.noopAll();
        tnodeBinarySendpool.noopAll();
    }
}
