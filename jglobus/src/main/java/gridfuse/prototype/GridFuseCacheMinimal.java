package gridfuse.prototype;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * GridFuseCacheMinimal provides a basic caching approach
 * for Grid-FUSE, calls are made to the remote server
 * for each interaction with the store.
 *
 */
public class GridFuseCacheMinimal extends GridFuseCache {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );

    public GridFuseCacheMinimal() throws IOException, FileNotFoundException {
        LOGGER.info("Caching type: Minimal");
        init();
    }

    /**
     * Called when program shuts down, stop any extra threads in here.
     */
    public void stopAll() {
        return;
    }

    /**
     * Creates path directories on local drive,
     * thus the dir can be opened by the system normally.
     *
     * @param path describes where the dir is.
     * @return int 0 for everything being fine. Otherwise linux error code.
     */
    @Override
    public int opendir(String path) {
        try {
            FileStat file = dirTree.find_file(path);
            String type = file.getType();
            if (type.contains("dir")) {
                File dir = new File(localrootdir+path);
                dir.mkdirs();
                return 0;
            }
            return -20;
        } catch (ClassCastException ccE) {
            //Object returned wasn't a FileStat, it must be a String.
            //Search the server for the requested dir.
            readdir(path);
            try {
                FileStat file = dirTree.find_file(path);
                String type = file.getType();
                if (type.contains("dir")) {
                    File dir = new File(localrootdir+path);
                    dir.mkdirs();
                    return 0;
                }
                return -20;
            } catch (ClassCastException ccE2) {
                return -2;
            }
        }
    }

    /**
     * Queries the GridFTP server for a list of files and metadata in the directory given in path.
     * Updates the cached directory tree with metadata.
     *
     * @param path describes the location of the directory to be read.
     * @return FileStat[] Array of FileStat objects holding the contents of the requested directory,
     * or single FileStat object in array containing error code if errors occur.
     */
    public FileStat[] readdir(String path) {
        //Request an update to dirTree from remote server
        int retstat = gridOperation.mlsd(path);
        if (retstat == -20) {
            LOGGER.fine("Not a directory, moving up.");
            path = path.substring(0,path.lastIndexOf("/"));
            retstat = gridOperation.mlsd(path);
        }
        if (retstat != 0) {
            return new FileStat[] {new FileStat(new File(path).getName(),retstat)};
        }
        //Get latest directory contents from dirTree
        return dirTree.cached_readdir(path);
    }

    /**
     * Called by FUSE when release call is made.
     * File is written back to mainstore if it has been modified.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. Otherwise linux error code.
     */
    public int release(String path) {
        File file = new File(localrootdir+path);
        FileStat fileMeta;
        try {
            fileMeta = dirTree.find_file(path);
        } catch (ClassCastException ccE) {
            LOGGER.warning("ClassCastException in release()");
            String fileName = file.getName();
            path = path.substring(0,path.lastIndexOf("/"));
            //This exception suggests that the file doesn't exit on the remote
            //server, so send it there.
            return write_back(path,fileName);
        }
        //If the local file has been edited more recently than the remote file, send it back
        if ((file.lastModified()/1000 > fileMeta.getMTime()) || (fileMeta.getCacheStatus() == FileStat.CACHE_AHEAD)) {
            String fileName = file.getName();
            path = path.substring(0,path.lastIndexOf("/"));
            int retstat = write_back(path,fileName);
            if (retstat == 0) { fileMeta.setMTime(file.lastModified()/1000); }
            return retstat;
        }
        else {
            return 0;
        }
    }

    /**
     * Called by FUSE when write call is made.
     * Updates file metadata in tree to inform of changes to be written back.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine.
     */
    public int write(String path) {
        try {
            FileStat fileMeta = dirTree.find_file(path);
            //If we have write access
            if ( access(fileMeta,02) == 0 ) {
                //Mark the local file as ahead of the remote store
                fileMeta.setCacheStatus(FileStat.CACHE_AHEAD);
                return 0;
            }
        } catch (ClassCastException ccE) {
            LOGGER.warning("Cannot mark changes to be written back.\n\tUserObject in tree not a FileStat.");
        }
        return -13;
    }

    /**
     * Copies local file to main store.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int write_back(String path, String fileName) {
        int retstat = 0;
        //If we have write access
        if ( access(path+"/"+fileName,02) == 0 ) {
            //Send the file to the remote store
            retstat = gridOperation.fileTransfer(path+"/"+fileName,FileStat.CACHE_AHEAD);
        }
        else {
            retstat = -13;
        }
        if (retstat == 0) {
            try {
                FileStat file = dirTree.find_file(path);
                //Mark the local file as in sync with the remote store
                file.setCacheStatus(FileStat.CACHE_SYNCED);
            } catch (ClassCastException ccE) {
                LOGGER.warning("Cannot mark as changes written back.\n\tUserObject in tree not a string.");
            }
        }
        return retstat;
    }
}
