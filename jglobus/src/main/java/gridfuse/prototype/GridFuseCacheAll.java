package gridfuse.prototype;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * GridFuseCacheAll periodically caches everything
 * and writes everything back to the remote store.
 *
 */
public class GridFuseCacheAll extends GridFuseCache {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );
    ScheduledExecutorService scheduledMLSR;
    ScheduledExecutorService scheduledNOOP;
    ExecutorService queueSkim;
    ExecutorService transferGetExec;
    ExecutorService transferSendExec;
    ConcurrentLinkedQueue<String> getQueue;
    ConcurrentLinkedQueue<String> sendQueue;

    public GridFuseCacheAll() throws IOException, FileNotFoundException {
        LOGGER.info("Caching type: All");
        init();

        scheduledMLSR = Executors.newScheduledThreadPool(1);
        scheduledNOOP = Executors.newScheduledThreadPool(1);
        transferGetExec = Executors.newFixedThreadPool(maxPoolSize);
        transferSendExec = Executors.newFixedThreadPool(maxPoolSize);

        getQueue = new ConcurrentLinkedQueue();
        sendQueue = new ConcurrentLinkedQueue();
        //Prevents us from copying the same file twice at the same time
        ConcurrentLinkedQueue<String> inProgressQueue = new ConcurrentLinkedQueue();

        Runnable mlsr = () -> {
            //Check through entire directory tree of mainstore
            //and add contents to dirTree.
            gridOperation.mlsr("/");
            //Add every unsynced item to the appropriate queue
            getQueue.addAll(dirTree.getUnSyncedPaths("/",-1,FileStat.CACHE_BEHIND));
            sendQueue.addAll(dirTree.getUnSyncedPaths("/",-1,FileStat.CACHE_AHEAD));
        };

        int lsDelay;
        int lsFrequency;
        try {
            lsDelay = Integer.parseInt(GridFuseProps.get("lsDelay"));
        } catch (NumberFormatException nfE) {
            lsDelay = 3;
            LOGGER.log(Level.INFO,"NumberFormatException while parsing lsDelay, using default of: "+lsDelay, nfE);
        }
        try {
            lsFrequency = Integer.parseInt(GridFuseProps.get("lsFrequency"));
        } catch (NumberFormatException nfE2) {
            lsFrequency = 30;
            LOGGER.log(Level.INFO,"NumberFormatException while parsing lsFrequency, using default of: "+lsFrequency, nfE2);
        }
        scheduledMLSR.scheduleWithFixedDelay(mlsr, lsDelay,lsFrequency, TimeUnit.SECONDS);

        int tempNum;
        try {
            tempNum = Integer.parseInt(GridFuseProps.get("numberOfFilesToTransfer"));
        } catch (NumberFormatException nfE3) {
            tempNum = 10;
            LOGGER.log(Level.INFO,"NumberFormatException while parsing numberOfFileToTransfer, using default of: "+tempNum, nfE3);
        }
        final int numberOfFilesToTransfer = tempNum;

        Runnable get = () -> {
            String path;
            String[] paths = new String[numberOfFilesToTransfer];
            synchronized ( getQueue ) {
                for (int i = 0; i < numberOfFilesToTransfer; i++) {
                    path = getQueue.poll();
                    if (path != null) {
                        //Remove all instances of this path in the queue
                        ArrayList<String> toremove = new ArrayList<String>();
                        toremove.add(path);
                        getQueue.removeAll(toremove);
                        LOGGER.finer("Removing "+path+" from getQueue");

                        synchronized ( inProgressQueue ) {
                            //Make sure file is not currently being transfered
                            if (!inProgressQueue.contains(path)) {
                                inProgressQueue.add(path);
                            }
                            else {
                                //If the file is already being transfered, leave it until the next pass
                                LOGGER.finest("File "+path+" already being transferred");
                                return;
                            }
                        }
                        paths[i] = path;
                    } else {
                        if (paths[0] == null) {
                            return;
                        }
                        else {
                            break;
                        }
                    }
                }
            }
            multipleFileTransfer(paths,FileStat.CACHE_BEHIND);
            LOGGER.finer("Finished transfering a bunch of files");
            inProgressQueue.removeAll(new ArrayList<String>(Arrays.asList(paths)));
        };
        Runnable send = () -> {
            String path;
            String[] paths = new String[numberOfFilesToTransfer];
            synchronized ( sendQueue ) {
                for (int i = 0; i < numberOfFilesToTransfer; i++) {
                    path = sendQueue.poll();
                    if (path != null) {
                        //Remove all instances of this path in the queue
                        ArrayList<String> toremove = new ArrayList<String>();
                        toremove.add(path);
                        sendQueue.removeAll(toremove);
                        LOGGER.finer("Removing "+path+" from sendQueue");

                        synchronized ( inProgressQueue ) {
                            //Make sure file is not currently being transfered
                            if (!inProgressQueue.contains(path)) {
                                inProgressQueue.add(path);
                            }
                            else {
                                //If the file is already being transfered, leave it until the next pass
                                LOGGER.finest("File "+path+" already being transferred");
                                return;
                            }
                        }
                        paths[i] = path;
                    } else {
                        if (paths[0] == null) {
                            return;
                        }
                        else {
                            break;
                        }
                    }
                }
            }
            multipleFileTransfer(paths,FileStat.CACHE_AHEAD);
            LOGGER.finer("Finished transfering a bunch of files");
            inProgressQueue.removeAll(new ArrayList<String>(Arrays.asList(paths)));
        };
        //TODO: Look at this again, does this thread need to exist or can we rearrange the get and send threads?
        //Start a thread that skims tasks off the caching queues and submits them to thread pools
        queueSkim = Executors.newSingleThreadExecutor();
        queueSkim.submit(() -> {
            while(!queueSkim.isShutdown()) {
                transferGetExec.submit(get);
                transferSendExec.submit(send);
            }
        });

        Runnable noop = () -> {
            gridOperation.noop();
        };

        int noopFrequency;
        try {
            noopFrequency = Integer.parseInt(GridFuseProps.get("noopFrequency"));
        } catch (NumberFormatException nfE4) {
            noopFrequency = 540;
            LOGGER.log(Level.INFO,"NumberFormatException while parsing noopFrequency, using default of: "+noopFrequency, nfE4);
        }
        //Schedule a NOOP command to the server, but use noopFrequency as the delay,
        //it doesn't need to happen until that much time has passed.
        scheduledNOOP.scheduleWithFixedDelay(noop, noopFrequency, noopFrequency, TimeUnit.SECONDS);

        return;
    }

    /**
     * Shutdown all the ExecutorServices.
     *
     * Remember to add them to the list here.
     */
    public void stopAll() {
        //TODO: These shutdown times should probably be in a config file.
        stopExecutorService(scheduledNOOP, "NOOP executor", 2);
        stopExecutorService(scheduledMLSR, "MLSR executor", 5);
        stopExecutorService(queueSkim, "Queue Skim executor", 10);
        stopExecutorService(transferGetExec, "File transfer get executor", 5);
        stopExecutorService(transferSendExec, "File transfer send executor", 30);
    }

    /**
     * Requests the shutdown of given ExecutorService.
     *
     * If the service doesn't shutdown within [timeout] seconds,
     * then the service is forced to shutdown with shutdownNow().
     *
     * @param exec This is the executor you wish to shutdown.
     * @param name A name for your executor, for terminal output only.
     * @param timeout How long you are willing to wait for the executor to shut itself down.
     */
    private void stopExecutorService(ExecutorService exec, String name, int timeout) {
        try {
            LOGGER.info("Attempting to shutdown "+name);
            //Patiently request shutdown
            exec.shutdown();
            exec.awaitTermination(timeout, TimeUnit.SECONDS);
        }
        catch (InterruptedException iE) {
            LOGGER.warning("Task shutting down "+name+" interrupted");
        }
        finally {
            if (!exec.isTerminated()) {
                LOGGER.severe(name+" is taking longer than "+timeout+" seconds to shutdown");
            }
            //Instant shutdown
            exec.shutdownNow();
            LOGGER.info(name+" shutdown");
        }
    }

    /**
     * Queries the cached directory tree for a list of files and metadata
     * in the directory given in path.
     *
     * @param path describes the location of the directory to be read.
     * @return FileStat[] Array of FileStat objects holding the contents of the requested directory.
     */
    public FileStat[] readdir(String path) {
        return dirTree.cached_readdir(path);
    }

    /**
     * Deletes a file from the GridFTP server and removes it from the cached directory tree.
     * Also removes it from the file transfer queues.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     * -2 when there's a ServerException (probably that the file or directory doesn't exist)
     */
    @Override
    public int unlink(String path) {
        //Delete from remote store
        int retstat = gridOperation.unlink(path);
        //Remove from dirTree
        dirTree.delete(path);

        //Remove file from file transfer queues
        ArrayList<String> toremove = new ArrayList<String>();
        toremove.add(path);
        getQueue.removeAll(toremove);
        sendQueue.removeAll(toremove);
        return retstat;
    }

    /**
     * Called by FUSE when release call is made.
     *
     * @param path describes where the file is.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int release(String path) {
        FileStat fileMeta = dirTree.find_file(path);
        File file = new File(localrootdir+path);
        //Update the file size parameter
        fileMeta.setSize(String.valueOf(file.length()));
        return 0;
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
     * Copies a single file to/from mainstore.
     *
     * @param path describes where the file is.
     * @param direction Copy to mainstore: FileStat.CACHE_AHEAD.
     *                  Copy from mainstore: FileStat.CACHE_BEHIND.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int fileTransfer(String path, int direction) {
        FileStat file = null;
        try {
            LOGGER.fine("Marking "+path+" as up to date");
            file = dirTree.find_file(path);
            file.setCacheStatus(FileStat.CACHE_SYNCED);
        } catch (ClassCastException ccE) {
            LOGGER.warning("Cannot mark as changes written back or update remote mtime.\n\tUserObject in tree not a string.");
        }
        //Transfer file
        int retstat = gridOperation.fileTransfer(path,direction);
        if (retstat == 0) {
            if (file != null) {
                //If we updated the remote file, set its modification
                //time to be the same as the local file so it doesn't
                //get sent back and forth
                if (direction == FileStat.CACHE_AHEAD) {
                    changeModificationTime(path,file.getMTime());
                }
            }
        }
        else if (retstat == -2) {
            LOGGER.finer("File doesn't exist, removing from dirTree");
            //If the file doesn't exist, then it shouldn't be in the dirTree
            dirTree.delete(path);
        }
        else {
            LOGGER.severe("Transfer of "+path+" failed.");
            if (file != null) {
                LOGGER.fine("Marking "+path+" as not synced.");
                file.setCacheStatus(direction);
            }
        }
        return retstat;
    }

    /**
     * Copies multiple files to/from mainstore.
     *
     * @param path describes where the file is.
     * @param direction Copy to mainstore: FileStat.CACHE_AHEAD.
     *                  Copy from mainstore: FileStat.CACHE_BEHIND.
     * @return int 0 for everything being fine. -1 when there's an error.
     */
    public int multipleFileTransfer(String[] paths, int direction) {
        ArrayList<FileStat> files = new ArrayList<FileStat>();
        for(String path : paths) {
            if ( path != null ) {
                FileStat file = null;
                try {
                    LOGGER.fine("Marking "+path+" as up to date");
                    file = dirTree.find_file(path);
                    file.setCacheStatus(FileStat.CACHE_SYNCED);
                    files.add(file);
                } catch (ClassCastException ccE) {
                    LOGGER.warning("Cannot mark as changes written back or update remote mtime.\n\tUserObject in tree not a string.");
                }
            }
        }
        //Transfer file
        int retstat = gridOperation.multipleFileTransfer(paths,direction);
        // TODO: Work out what happens with the return value here
        if (retstat == 0) {
            //If we updated the remote files, set their modification
            //time to be the same as the local files so they don't
            //get sent back and forth
            for(String path : paths) {
                if (direction == FileStat.CACHE_AHEAD) {
                    try {
                        changeModificationTime(path,dirTree.find_file(path).getMTime());
                    } catch (ClassCastException ccE) {
                        LOGGER.finer("Cannot get FileStat to set MTime on remote store for "+path);
                    }
                }
            }
        }
        else if (retstat == -2) {
            LOGGER.finer("One of the files doesn't exist, removing from dirTree\n\n\n???????????????????????????\n\n\n");
            //If the file doesn't exist, then it shouldn't be in the dirTree
            for(String path : paths) {
                if ( path != null ) {
                    dirTree.delete(path);
                }
            }
        }
        else {
            for(String path : paths) {
                if ( path != null ) {
                    LOGGER.severe("Transfer of "+path+" failed.");
                }
            }
            for(FileStat file : files) {
                LOGGER.fine("Marking "+file.toString()+" as not synced.");
                file.setCacheStatus(direction);
            }
        }
        return retstat;
    }

}
