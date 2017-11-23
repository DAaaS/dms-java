package gridfuse.prototype;

import java.io.File;
import java.io.IOException;
import org.globus.ftp.MlsxEntry;
import org.globus.ftp.MlsxEntryWriter;
import java.util.TimeZone;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;

/**
 * This provides the object to send to globus's MLSR command.
 * It can take the metadata returned and send it to the directory
 * tree to be added in the correct place.
 */
public class GridFuseMlsxEntryWriter implements MlsxEntryWriter {
    GridFuseDirTree dirTree;

    /**
     * Constructor, pass in the directory tree object to which
     * the metadata should be added.
     *
     * @param dirTree the directory tree object.
     */
    public GridFuseMlsxEntryWriter(GridFuseDirTree dirTree) {
        this.dirTree = dirTree;
    }

    /**
     * Writes a single entry from the stream.
     * Parses the MlsxEntry filename and adds it to the 
     * GridFuseDirTree passed to this object's constructor
     *
     * @param entry the file/directory entry
     */
    public void write(MlsxEntry entry) throws IOException {
        String fullPath = entry.getFileName().toString();
        String path = new File(fullPath).getParent();
        if (path == null) { path = "/"; }
        String fileName = new File(fullPath).getName();

        DateFormat dfm = new SimpleDateFormat("yyyyMMddHHmmss");
        dfm.setTimeZone(TimeZone.getTimeZone("GMT"));
        long mtime;
        try { mtime = dfm.parse(entry.get(MlsxEntry.MODIFY)).getTime()/1000;
        } catch (ParseException e) { mtime = 0L; }

        //Create file entry
        FileStat fileMeta = new FileStat(fileName,
                                    entry.get(MlsxEntry.TYPE),
                                    entry.get(MlsxEntry.UNIX_MODE),
                                    entry.get(MlsxEntry.SIZE),
                                    mtime,
                                    entry.get(MlsxEntry.UNIX_OWNER),
                                    entry.get(MlsxEntry.UNIX_GROUP),
                                    FileStat.CACHE_BEHIND);
        dirTree.add_node(path,fileMeta);
        return;
    }

    /**
     * Notifies the writer that the stream of entries has ended.
     *
     */
    public void close() {
        return;
    }
}
