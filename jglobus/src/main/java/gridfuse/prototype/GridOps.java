package gridfuse.prototype;

/**
 * Interface for performing remote filesystem operations on a server.
 */
interface GridOps {
    /**
     * Transfers a file to or from remote server, depending on direction parameter.
     *
     * @param path, directory path
     * @param direction, FileStat.CACHE_BEHIND = get file from remote server.
     *                  FileStat.CACHE_AHEAD = send file to remote server.
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int fileTransfer(String path, int direction);

    /**
     * Transfers files to or from remote server, depending on direction parameter.
     *
     * @param path, directory path
     * @param direction, FileStat.CACHE_BEHIND = get files from remote server.
     *                  FileStat.CACHE_AHEAD = send files to remote server.
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int multipleFileTransfer(String[] path, int direction);

    /**
     * Creates a remote directory.
     *
     * @param path, directory path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int mkdir(String path);

    /**
     * Deletes remote file.
     *
     * @param path, file path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int unlink(String path);

    /**
     * Deletes remote directory.
     *
     * @param path, directory path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int rmdir(String path);

    /**
     * Performs rename operation on remote file.
     *
     * @param path, old file path
     * @param newpath, new file path
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int rename(String path, String newpath);

    /**
     * Changes the modification time of a file on the remote server.
     *
     * @param path, path to file to be changed
     * @param mtime, new modification time to be set, in microseconds.
     *
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int changeMTime(String path, long mtime);

    /**
     * Searches through remote directory (path) and adds the
     * results to dirTree
     *
     * @param path the path to the directory you want to list
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int mlsd(String path);

    /**
     * Recursively searches through remote directory (path) and adds the
     * results to dirTree
     *
     * @param path the path to the top level directory you want to list from.
     * @return int 0 for everything being fine, otherwise linux error codes.
     */
    public int mlsr(String path);

    /**
     * Performs a NOOP request to keep connections alive.
     */
    public void noop();
}
