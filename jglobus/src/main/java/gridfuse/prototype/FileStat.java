package gridfuse.prototype;


//TODO: Add some comments to this class
public class FileStat {
    public static final int CACHE_BEHIND = -1;
    public static final int CACHE_SYNCED = 0;
    public static final int CACHE_AHEAD = 1;

    private String filename = "";
    private String type = "";
    private String permissions = "";
    private String owner = "root";
    private String group = "root";
    private String size = "";
    private long mtime = 0L;
    private long nlink = 1L;
    private int errorcode = 0;
    private int cacheStatus = CACHE_BEHIND;

    /**
     * Use this constructor for reporting errors
     * back to FUSE.
     *
     * @param errorcode Error code.
     */
    public FileStat(String newfilename, int newerrorcode) {
        this.filename = newfilename;
        this.errorcode = newerrorcode;
    }
    public FileStat(String newfilename, String newtype, String newpermissions) {
        this.filename = newfilename;
        this.type = newtype;
        this.permissions = newpermissions;
    }
    public FileStat(String newfilename, String newtype, String newpermissions, String newsize, long newmtime) {
        this.filename = newfilename;
        this.type = newtype;
        this.permissions = newpermissions;
        this.size = newsize;
        this.mtime = newmtime;
    }
    public FileStat(String newfilename, String newtype, String newpermissions, String newsize, long newmtime, String newowner, String newgroup, int newcacheStatus) {
        this.filename = newfilename;
        this.type = newtype;
        this.permissions = newpermissions;
        this.size = newsize;
        this.mtime = newmtime;
        this.owner = newowner;
        this.group = newgroup;
        this.cacheStatus = newcacheStatus;
        if (!this.type.equals("file") ) {
            this.nlink = 2;
        }
    }
    public FileStat(FileStat original) {
        this.filename = original.getFilename();
        this.type = original.getType();
        this.permissions = original.getPermissions();
        this.size = original.getSize();
        this.mtime = original.getMTime();
        this.owner = original.getOwner();
        this.group = original.getGroup();
        this.cacheStatus = original.getCacheStatus();
        this.nlink = original.getNLink();
    }
    public String getFilename() {
        return filename;
    }
    public void setFilename(String newfilename) {
        this.filename = newfilename;
        return;
    }
    public String getType() {
        return type;
    }
    public void setType(String newtype) {
        this.type = newtype;
        return;
    }
    public String getPermissions() {
        return permissions;
    }
    public String getOwner() {
        return owner;
    }
    public String getGroup() {
        return group;
    }
    public String getSize() {
        return size;
    }
    public void setSize(String newsize) {
        this.size = newsize;
        return;
    }
    public long getMTime() {
        return mtime;
    }
    public void setMTime(long newmtime) {
        this.mtime = newmtime;
        return;
    }
    public long getNLink() {
        return nlink;
    }
    public void setNLink(long newnlink) {
        this.nlink = newnlink;
        return;
    }
    public void incrementNLink() {
        this.nlink++;
        return;
    }
    public void decrementNLink() {
        this.nlink--;
        if ( this.nlink < 0 ) {
            this.nlink = 0;
        }
        return;
    }
    public int getErrorCode() {
        return errorcode;
    }
    public int getCacheStatus() {
        return cacheStatus;
    }
    public void setCacheStatus(int newcacheStatus) {
        this.cacheStatus = newcacheStatus;
        return;
    }
    @Override
    public String toString() {
        return getFilename();
    }
}
