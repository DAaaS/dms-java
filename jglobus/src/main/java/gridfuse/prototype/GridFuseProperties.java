package gridfuse.prototype;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.File;
import java.util.Properties;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GridFuseProperties {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );
    InputStream inputStream;
    private HashMap<String, String> GridFuseProps = new HashMap<String, String>();
    
    /**
     * Reads config.properties and returns contents as HashMap
     */
    public HashMap<String,String> getProperties() throws IOException, FileNotFoundException {
        Properties prop = new Properties();
        String propFilename = "config.properties";
        //Use properties file in resources dir as standard
        String propDir = "../../jglobus/resources/";
        String propFile = propDir+propFilename;
        
        //If config file exists in home dir, use that, otherwise check /etc/gridfuse/
        String envProp = System.getenv("GRID_FUSE_CONF")+"/"+propFilename;
        String homeProp = System.getProperty("user.home")+"/"+propFilename;
        String etcProp = "/etc/gridfuse/"+propFilename;

        if ((new File(envProp)).exists()) {
            propFile = envProp;
        }
        else if ((new File(homeProp)).exists()) {
            propFile = homeProp;
        }
        else if ((new File(etcProp)).exists()) {
            propFile = etcProp;
        }
        LOGGER.info("Using "+propFile+" for config");
        //Add config directory to GridFuseProps
        GridFuseProps.put("GRID_FUSE_CONF",new File(propFile).getParent());

        inputStream = new FileInputStream(propFile);

        if (inputStream != null) {
            prop.load(inputStream);
            Enumeration en = prop.propertyNames();
            String key;
            while (en.hasMoreElements()) {
                key = (String) en.nextElement();
                GridFuseProps.put(key,prop.getProperty(key));
            }
        } else {
            throw new FileNotFoundException("property file '" + propFilename + "' not foundin the classpath");
        }
        inputStream.close();
        return GridFuseProps;
    }
}
