package gridfuse.prototype;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The GridFuseCache provides a callback interface for creating
 * different caching approaches for Grid-FUSE.
 *
 */
public abstract class GridFuseCache {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );

    //Unparsed config
    protected HashMap<String,String> GridFuseProps;

    //Parsed config
    protected String localrootdir;
    protected String mainstorerootdir;
    protected String cacherootdir;
    protected String user;
    protected String group;
    protected int maxPoolSize;

    //Directory tree
    GridFuseDirTree dirTree;

    //Remote server operations
    GridOps gridOperation;


    /**
     * Initialises GridFTP settings from config.properties
     */
    public void init() throws IOException, FileNotFoundException {
        GridFuseProperties properties = new GridFuseProperties();
        GridFuseProps           = properties.getProperties();
        localrootdir            = GridFuseProps.get("localrootdir");
        cacherootdir            = GridFuseProps.get("cacherootdir");
        mainstorerootdir        = GridFuseProps.get("mainstorerootdir");
        user                    = GridFuseProps.get("user");
        group                   = GridFuseProps.get("group");

        maxPoolSize         = Integer.parseInt(GridFuseProps.get("maxPoolSize"));

        dirTree = new GridFuseDirTree();
        gridOperation = new GridOpsGridFTP(GridFuseProps,dirTree);

        return;
    }

    /**
     * Called when program shuts down, stop any extra threads in here.
     */
    public abstract void stopAll();

    /**
     * Searches the cached directory tree for the file given in path, returns its metadata
     * Runs readdir() if the file's metadata isn't in the tree.
     *
     * @param path describes the location of the file.
     * @return FileStat object containing metadata if found, null otherwise.
     */
    public FileStat getattr(String path) {
        try {
            FileStat file = dirTree.find_file(path);
            return file;
        } catch (ClassCastException e) {
            //Object returned wasn't a FileStat, it must be a String.
            String fileName = new File(path).getName();
            //When path = "/", fileName is empty, so fix that.
            if (fileName.equals("")) { fileName = "/"; }
            //Search the server for the requested file.
            FileStat[] files = readdir(path);
            if (files != null) {
                for (FileStat file : files) {
                    if ( fileName.equals(file.toString()) ) {
                        return file;
                    }
                }
            }
            return null;
        }
    }

    /**
     * Creates path directories on local drive,
     * thus the dir can be opened by the system normally.
     *
     * Override this to update the dirTree when you aren't certain
     * the dirTree will be precached.
     *
     * @param path describes where the dir is.
     * @return int 0 for everything being fine. Otherwise linux error code.
     */
    public int opendir(String path) {
        try {
            FileStat file = dirTree.find_file(path);
            String type = file.getType();
            if (type.contains("dir")) {
                File dir = new File(localrootdir+path);
                //Make the parent directories on the local
                //system which lead to this directory
                dir.mkdirs();
                return 0;
            }
            return -20;
        } catch (ClassCastException ccE) {
            return -2;
        }
    }

    /**
     * Queries the GridFTP server for a list of files and metadata in the directory given in path.
     * Updates the cached directory tree with metadata.
     *
     * @param path describes the location of the directory to be read.
     * @return FileStat[] Array of FileStat objects holding the contents of the requested directory,
     * or null if errors occur.
     */
    public abstract FileStat[] readdir(String path);

    /**
     * Creates path directories on local drive, copies remote file to the local path
     * thus the file can be opened by the system normally.
     * Will probably need a better solution for large files.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. Linux error code otherwise.
     */
    public int open(String path) {
        String fileName = new File(path).getName();
        File file = new File(localrootdir+path);
        if (file.exists()) {
            //If the file is already cached return 17 (EEXIST in linux)
            //This might not be very wise but it's easier right now.
            return 17;
        }

        //Copy the file to the local cache
        int retstat = gridOperation.fileTransfer(path,FileStat.CACHE_BEHIND);

        //TODO: Make this less terrible
        //Wait until the file exists before continuing. I don't like this.
        int count = 0;
        while(!file.exists()) {
            count++;
            if (count > 1000000) {
                LOGGER.warning("Given up waiting for file to exist.");
                break;
            }
        }
        return retstat;
    }

    /**
     * Updates the tree with the new file.
     *
     * @param path describes where the file should be created.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int mknod(String path) {
        File file = new File(path);
        FileStat metaData = new FileStat(file.getName(),"file","0644","0",System.currentTimeMillis()/1000,user,group,FileStat.CACHE_AHEAD);
        dirTree.add_node(file.getParent(),metaData);
        return 0;
    }

    /**
     * Creates a directory on the GridFTP server.
     *
     * @param path describes where the directory should be created.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int mkdir(String path) {
        int retstat = gridOperation.mkdir(path);
        if (retstat == 0) {
            File file = new File(path);
            FileStat metaData = new FileStat(file.getName(),"dir","0755","0",System.currentTimeMillis()/1000,user,group,FileStat.CACHE_SYNCED);
            dirTree.add_node(file.getParent(),metaData);
        }
        return retstat;
    }

    /**
     * Deletes a file from the GridFTP server and removes it from the cached directory tree.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     * -2 when there's a ServerException (probably that the file or directory doesn't exist)
     */
    public int unlink(String path) {
        int retstat = gridOperation.unlink(path);
        dirTree.delete(path);
        return retstat;
    }

    /**
     * Deletes a directory from the GridFTP server and removes it from the cached directory tree.
     *
     * @param path describes where the directory is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int rmdir(String path) {
        int retstat = gridOperation.rmdir(path);
        if (retstat == 0) {
            dirTree.delete(path);
        }
        return retstat;
    }

    /**
     * Renames a file (path) and/or moves it to another directory (newpath), 
     * then updates cached directory tree.
     *
     * @param path describes where the file is.
     * @param newpath describes where the file should be moved to.
     * @return int 0 for everything being fine. -2 when there's a
     * ServerException or IOException
     * (probably that the file or directory doesn't exist)
     */
    public int rename(String path, String newpath) {
        //TODO: make use of the retstat here.

        //Move and/or rename file on server
        int retstat = gridOperation.rename(path,newpath);

        File file = new File(newpath);
        //Find old location on tree and remove it from there
        FileStat metaDatum;
        try {
            metaDatum = dirTree.find_file(path);
        } catch (ClassCastException ccE) {
            metaDatum = null;
        }
        dirTree.delete(path);
        //Put file in new location on tree
        if (metaDatum == null) {
            //If we didn't have a FileStat object earlier,
            //then we won't mind adding a string now.
            dirTree.find_file(newpath);
        }
        else {
            //Rename metaDatum
            metaDatum.setFilename(file.getName());
            //Add FileStat object to new spot
            dirTree.add_node(file.getParent(),metaDatum);
        }

        //Create the local directories for the new path
        File localfile = new File(localrootdir+newpath);
        localfile.getParentFile().mkdirs();
        return 0;
    }


    /**
     * Called by FUSE when release call is made
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public abstract int release(String path);

    /**
     * Called by FUSE when write call is made.
     * Updates file metadata in tree to inform of changes to be written back.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine.
     */
    public abstract int write(String path);
    
    /**
     * Check if file given in path is accessible with mode
     * given by mask.
     *
     * Searches for the file in the dirTree, if it finds a
     * FileStat object, move on to the other access method
     * to check the permissions.
     *
     * @param path describes where the file is.
     * @param mask describes the access to be checked.
     * @return int 0 for access granted, -1 otherwise.
     */
    public int access(String path, int mask) {
        FileStat file;
        try {
            file = dirTree.find_file(path);
        } catch (ClassCastException e) {
            if ( mask == 0 ) {
                return -1;
            }
            return 0;
        }
        if ( file != null ) {
            //Go check the FileStat permissions against the mask
            return access(file, mask);
        }
        else {
            return 0;
        }
    }

    /**
     * Check if file given is accessible with mode
     * given by mask.
     *
     * Does a bitwise AND on the file permissions and
     * the mask, if the operation result equals the mask,
     * permission granted.
     *
     * @param file FileStat object representing the file.
     * @param mask describes the access to be checked.
     * @return int 0 for access granted, -1 otherwise.
     */
    public int access(FileStat file, int mask) {
        //Mask value of 0 means test for file existence
        if (mask == 0 && file != null) {
            return 0;
        }
        if (mask > 7 || mask < 0) {
            throw new IllegalArgumentException("Mask passed to access is out of bounds: "+mask);
        }
        //Separate out permission bits
        int[] perms = {0,0,0,0};
        for ( int i=1; i < 4; i++ ) {
            perms[i] = Integer.parseInt(Character.toString(file.getPermissions().charAt(i)),8);
        }
        if ( file.getOwner().equals(user) ) {
            if ( (perms[1] & mask) == mask ) {
                //Owner has permission
                return 0;
            }
        }
        else if ( file.getGroup().equals(group) ) {
            if ( (perms[2] & mask) == mask ) {
                //Group has permission
                return 0;
            }
        }
        else if ( (perms[3] & mask) == mask ) {
            //Other has permission
            return 0;
        }
        LOGGER.info("Permission denied");
        return -1;
    }
    
    /**
     * Sets mtime on the remote file.
     * 
     * @param path describes where the file is.
     * @param mtime the new modification time in seconds.
     */
    public void changeModificationTime(String path, long mtime) {
        gridOperation.changeMTime(path,mtime);
        return;
    }

}
