package gridfuse.prototype;

import org.globus.util.ConfigUtil;
import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This just extends ConfigUtil so we can define
 * where we get our credential from.
 */
public class GridFuseCredHelper extends ConfigUtil {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );
    private static final String PROXY_NAME = "x509up_u";

    /**
     * Get Credential from /tmp/
     */
    public GSSCredential getDefaultCredential() throws IOException, GSSException {
        return this.getCredential(new File(discoverProxyLocation()));
    }

    /**
     * Get Credential from path.
     *
     * @param path directory containing credential
     */
    public GSSCredential getCredentialFrom(String path) throws IOException, GSSException {
        return this.getCredential(new File(discoverProxyLocation(path)));
    }

    /**
     * Opens up proxyFile and creates a GSSCredential.
     *
     * @param proxyFile File representing credential
     */
    public GSSCredential getCredential(File proxyFile) throws IOException, GSSException {
        byte[] proxyBytes = new byte[(int) proxyFile.length()];
        FileInputStream in = new FileInputStream(proxyFile);
        try {
            in.read(proxyBytes);
        } finally {
            in.close();
        }
        ExtendedGSSManager manager = (ExtendedGSSManager) ExtendedGSSManager.getInstance();
        return manager.createCredential(proxyBytes, ExtendedGSSCredential.IMPEXP_OPAQUE,
        GSSCredential.DEFAULT_LIFETIME, null, GSSCredential.INITIATE_AND_ACCEPT);
    }

    /**
     * Tries to discover user proxy location.
     * If a UID system property is set, and running on a Unix machine it
     * returns /tmp/x509up_u${UID}. If any other machine then Unix, it returns
     * ${tempdir}/x509up_u${UID}, where tempdir is a platform-specific
     * temporary directory as indicated by the java.io.tmpdir system property.
     * If a UID system property is not set, the username will be used instead
     * of the UID. That is, it returns ${tempdir}/x509up_u_${username}
     *
     * @return Absolute path to credential
     */
    public static String discoverProxyLocation() {
        return discoverProxyLocation("/tmp/");
    }

    /**
     * Tries to discover user proxy location based on the directory
     * you've provided.
     *
     * @param path directory containing credential
     * @return Absolute path to credential
     */
    public static String discoverProxyLocation(String path) {
        String uid = System.getProperty("UID");

        if (uid != null) {
            return getLocation(path, PROXY_NAME + uid);
        } else if (getOS() == UNIX_OS) {
            try {
                return getLocation(path, PROXY_NAME + getUID());
            } catch (IOException e) {
            }
        }

        /* If all else fails use username */
        String suffix = System.getProperty("user.name");
        if (suffix != null) {
            suffix = suffix.toLowerCase();
        } else {
            suffix = "nousername";
        }

        return getLocation(path, PROXY_NAME + "_" + suffix);
    }

    private static String getLocation(String path, String file) {
        File f = new File(path, file);
        return f.getAbsolutePath();
    }
}
