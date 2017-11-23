package gridfuse.prototype;

import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

public class App {
    public static GridFuseCache cache;
    private static final LogManager logManager = LogManager.getLogManager();
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );
    static{
        try {
            logManager.readConfiguration(new FileInputStream("../../jglobus/resources/logging.properties"));
        } catch (IOException ioE) {
            LOGGER.log(Level.SEVERE, "Error in loading logging config", ioE);
        }
    }

    /**
     * Main function, just used for testing right now.
     */
    public static void main(String[] args) {
        if ( init() != 0 ) {
            return;
        }
        try { Thread.sleep(40000); }
        catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        stopAll();
        return;
    }

    /**
     * Initialises GridFTP settings from config.properties
     */
    public static int init() {
        LOGGER.warning("Prototype");
        try {
            cache = new GridFuseCacheAll();
        } catch (FileNotFoundException ioE) {
            LOGGER.severe("Config file not found");
            return -13;
        } catch (IOException ioE) {
            LOGGER.severe("Missing certificate");
            return -13;
        }
        return 0;
    }

    /**
     * Stops all threads/operations.
     *
     * Hopefully safely.
     */
    public static void stopAll() {
        LOGGER.info("Stopping everything.");
        cache.stopAll();
        return;
    }

    //TODO: Check all the error codes that get back here are documented. E.g. -17 in open()

    /**
     * Creates path directories on local drive,
     * thus the dir can be opened by the system normally.
     *
     * @param path describes where the dir is.
     * @return int 0 for everything being fine. Otherwise linux error code.
     */
    public static int fuse_opendir(String path) {
        return cache.opendir(path);
    }

    /**
     * Queries the GridFTP server for a list of files and metadata in the directory given in path.
     * Updates the cached directory tree with metadata.
     *
     * @param path describes the location of the directory to be read.
     * @return FileStat[] Array of FileStat objects holding the contents of the requested directory,
     * or null if errors occur.
     */
    public static FileStat[] fuse_readdir(String path) {
        return cache.readdir(path);
    }

    /**
     * Searches the cached directory tree for the file given in path, returns its metadata
     * Runs fuse_readdir() if the file's metadata isn't in the tree.
     *
     * @param path describes the location of the file.
     * @return FileStat object containing metadata if found, null otherwise.
     */
    public static FileStat fuse_getattr(String path) {
        return cache.getattr(path);
    }

    /**
     * Creates path directories on local drive, copies remote file to the local path
     * thus the file can be opened by the system normally.
     * Will probably need a better solution for large files.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public static int fuse_open(String path) {
        return cache.open(path);
    }

    /**
     * Updates the tree with the new file.
     *
     * @param path describes where the file should be created.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public static int fuse_mknod(String path) {
        return cache.mknod(path);
    }

    /**
     * Creates a directory on the GridFTP server.
     *
     * @param path describes where the directory should be created.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public static int fuse_mkdir(String path) {
        return cache.mkdir(path);
    }
    
    /**
     * Deletes a file from the GridFTP server and removes it from the cached directory tree.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     * -2 when there's a ServerException (probably that the file or directory doesn't exist)
     */
    public static int fuse_unlink(String path) {
        return cache.unlink(path);
    }

    /**
     * Deletes a directory from the GridFTP server and removes it from the cached directory tree.
     *
     * @param path describes where the directory is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public static int fuse_rmdir(String path) {
        return cache.rmdir(path);
    }
    
    /**
     * Renames a file (path) and/or moves it to another directory (newpath), updates cached directory tree.
     * This should probably happen in a different order, ask the server first then update the tree.
     *
     * @param path describes where the file is.
     * @param newpath describes where the file should be moved to.
     * @return int 0 for everything being fine. -1 when there's an error.
     * -2 when there's a ServerException (probably that the file or directory doesn't exist)
     *  TODO: This needs updating once rename has been sorted
     */
    public static int fuse_rename(String path, String newpath) {
        return cache.rename(path,newpath);
    }

    /**
     * Called by FUSE when release call is made.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public static int fuse_release(String path) {
        return cache.release(path);
    }

    /**
     * Called by FUSE when write call is made.
     * Updates file metadata in tree to inform of changes to be written back.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine.
     */
    public static int fuse_write(String path) {
        return cache.write(path);
    }

    /**
     * Called by FUSE when access call is made.
     * Checks whether the access denoted by the mask is permitted,
     * or if the file exists (mask = 00)
     *
     * @param path describes where the file is.
     * @param mask describes the access to be checked.
     * @return int 0 for access granted, -1 otherwise.
     */
    public static int fuse_access(String path, int mask) {
        return cache.access(path,mask);
    }

}
