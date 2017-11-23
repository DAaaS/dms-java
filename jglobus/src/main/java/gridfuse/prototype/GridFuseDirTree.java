package gridfuse.prototype;

import java.util.Enumeration;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.tree.TreeNode;
import javax.swing.tree.DefaultMutableTreeNode;

/**
 * This holds the means to create and store a directory tree
 * structure and keep it up to date.
 */
public class GridFuseDirTree {
    private static final Logger LOGGER = Logger.getLogger( "GridFuseLogger" );

    //Metadata cache tree root node
    private DefaultMutableTreeNode root;

    /**
     * Constructor, creates root and its parent.
     */
    public GridFuseDirTree() {
        if (root == null) {
            root = new DefaultMutableTreeNode("/");
        }
    }

    /**
     * Adds file or directory metaData to the GridFuseDirTree.
     * Adds metadata entry to tree at specified path.
     *
     * @param path where the file goes in the tree.
     * @param metaDatum FileStat metadata.
     *
     * @return FileStat object which is now in the tree.
     */
    protected FileStat add_node(String path, FileStat metaDatum) {
        boolean file_in_tree;

        //Prepare tree
        DefaultMutableTreeNode current_dir = search_tree(path);

        //Deal with current and parent directories
        if( metaDatum.toString().equals(".") ) {
            //Update the current directory with the new details
            metaDatum.setFilename(current_dir.toString());
            try {
                //Try to keep the number of hard links when updating, otherwise don't worry.
                metaDatum.setNLink( ((FileStat) current_dir.getUserObject()).getNLink() );
            } catch (ClassCastException ccE) {
                //doesn't matter
            }
            current_dir.setUserObject( (Object) metaDatum );
        }
        else if( metaDatum.toString().equals("..") ) {
            //Update the parent directory with the new details
            if (!current_dir.isRoot()) {
                metaDatum.setFilename(((DefaultMutableTreeNode) current_dir.getParent()).toString());
                try {
                    //Try to keep the number of hard links from the parent when updating, otherwise don't worry.
                    metaDatum.setNLink( ((FileStat) ((DefaultMutableTreeNode) current_dir.getParent()).getUserObject()).getNLink() );
                } catch (ClassCastException ccE) {
                    //doesn't matter
                }
                ((DefaultMutableTreeNode) current_dir.getParent()).setUserObject( (Object) metaDatum);
            }
        }
        else {
            //Add to tree if it doesn't already exist, otherwise just update metadata
            Enumeration en = current_dir.children();
            file_in_tree = false;
            while (en.hasMoreElements()) {
                DefaultMutableTreeNode gotNode = (DefaultMutableTreeNode) en.nextElement();
                //Check we're on the right file
                if ( metaDatum.toString().equals(gotNode.toString())) {
                    //Make sure the node contains a FileStat object.
                    FileStat gotUserObject;
                    try {
                        gotUserObject = (FileStat) gotNode.getUserObject();
                        //Check if the remote file has been modified more recently
                        if ( metaDatum.getMTime() > gotUserObject.getMTime() ) {
                            gotUserObject.setCacheStatus(FileStat.CACHE_BEHIND);
                        }
                        //Only update the dirTree if the local file requires updating.
                        if (gotUserObject.getCacheStatus() == FileStat.CACHE_BEHIND) {
                            try {
                                //Try to keep the number of hard links when updating, otherwise don't worry.
                                metaDatum.setNLink( ((FileStat) gotNode.getUserObject()).getNLink() );
                            } catch (ClassCastException ccE) {
                                //doesn't matter
                            }
                            //Update existing node
                            gotNode.setUserObject( (Object) metaDatum);
                        }
                    }
                    catch (ClassCastException ccE) {
                        //Node only contained a string, replace with FileStat
                        gotNode.setUserObject( (Object) metaDatum);
                        if ( metaDatum.getType().equals("dir") ) {
                            ((FileStat)current_dir.getUserObject()).incrementNLink();
                        }
                    }
                    file_in_tree = true;
                    break;
                }
            }
            if(!file_in_tree) {
                //Node didn't exist, add it to tree
                current_dir.add(new DefaultMutableTreeNode(metaDatum));
                if ( metaDatum.getType().equals("dir") ) {
                    ((FileStat)current_dir.getUserObject()).incrementNLink();
                }
            }
        }
        return metaDatum;
    }

    /**
     * Searches through the cached directory tree down the given path
     * creates nodes as it goes if they do not already exist.
     * Created nodes contain a filename (String) as their userObject.
     *
     * @param path describes where the file should be located in the tree.
     * @return DefaultMutableTreeNode the node which represents the end of the given path
     * with its ancestors defined by the rest of the path.
     */
    protected DefaultMutableTreeNode search_tree(String path) {
        DefaultMutableTreeNode current_dir = root;
        boolean next_level;
        int pathdepth = path.split("/").length;
        //Breadth first search through the tree
        for ( int i=0;i<pathdepth;i++ ) {
            String dir = path.split("/")[i];
            if ( dir.equals("") ) {
                continue;
            }
            Enumeration en = current_dir.children();
            next_level = false;
            //Search current node's children for a match with this part of the path
            while (en.hasMoreElements()) {
                DefaultMutableTreeNode gotNode = (DefaultMutableTreeNode) en.nextElement();
                if( dir.equals(gotNode.toString()) ) {
                    current_dir = gotNode;
                    next_level = true;
                    break;
                }
            }
            if(next_level) { continue; }
            else {
                //If it runs out of tree before it gets to the end of the path,
                //start adding new nodes with just filenames
                for ( int j=i;j<pathdepth;j++ ) {
                    dir = path.split("/")[j];
                    DefaultMutableTreeNode newNode = new DefaultMutableTreeNode(dir);
                    current_dir.add(newNode);
                    current_dir = newNode;
                }
                break;
            }
        }
        return current_dir;
    }

    /**
     * Searches through the cached directory tree down the given path
     * creates nodes as it goes if they do not already exist.
     * Created nodes contain a filename (String) as their userObject.
     *
     * @param path describes where the file should be located in the tree.
     * @throws ClassCastException when node doesn't contain a FileStat object.
     * @return FileStat object, the metaData which represents the end of the given path.
     */
    protected FileStat find_file(String path) throws ClassCastException {
        return (FileStat) search_tree(path).getUserObject();
    }

    /**
     * Provides cached metaData for the files inside a given directory.
     *
     * @param path the path to the directory you want.
     * @return FileStat array containing cached metadata.
     */
    protected FileStat[] cached_readdir(String path) {
        DefaultMutableTreeNode dir = search_tree(path);
        ArrayList<FileStat> metasDatum = new ArrayList<FileStat>();
        int kids = dir.getChildCount();

        try {
            FileStat cdir = new FileStat((FileStat) dir.getUserObject());
            cdir.setType("cdir");
            metasDatum.add(cdir);
            kids++;
        } catch (ClassCastException ccE) {
            //I don't know why this might happen.
            LOGGER.warning("ClassCastException with file (current directory): " + dir.getUserObject().toString());
        }
        if (!dir.isRoot()) {
            try {
                FileStat pdir = new FileStat((FileStat) ((DefaultMutableTreeNode) dir.getParent()).getUserObject());
                pdir.setType("pdir");
                metasDatum.add(pdir);
                kids++;
            } catch (ClassCastException ccE2) {
                //This should only happen when we're opening the root directory,
                //in which case the parent should be the one on the local filesystem
                //anyway.
                LOGGER.fine("ClassCastException with file (parent directory): " + ((DefaultMutableTreeNode) dir.getParent()).getUserObject().toString());
            }
        }

        Enumeration en = dir.children();
        //Iterate through directory contents
        while (en.hasMoreElements()) {
            DefaultMutableTreeNode gotNode = (DefaultMutableTreeNode) en.nextElement();
            try {
                metasDatum.add((FileStat) gotNode.getUserObject());
            } catch (ClassCastException ccE3) {
                //This can happen with files which are only in the local cache.
                LOGGER.fine("ClassCastException with file: " + gotNode.getUserObject().toString());
                kids--;
            }
        }

        //Send back as an Array
        FileStat metaData[] = new FileStat[kids];
        printTree("/","\t",0);
        return metasDatum.toArray(metaData);
    }

    /**
     * Remove an entry from the dirTree.
     *
     * @param path That which is to be deleted.
     */
    protected void delete(String path) {
        //Get the nodes for the file and its parent
        DefaultMutableTreeNode fileNode = search_tree(path);
        DefaultMutableTreeNode pNode = (DefaultMutableTreeNode) fileNode.getParent();

        fileNode.removeFromParent();
        try {
            //See if this is a directory (not a regular file)
            if ( !((FileStat) fileNode.getUserObject()).getType().equals("file") ) {
                //Try to decrement the number of hard links to the parent directory
                try {
                    ((FileStat) pNode.getUserObject()).decrementNLink();
                } catch (ClassCastException ccE) {
                    LOGGER.fine("Parent node ("+pNode.toString()+") is not a FileStat object.");
                }
            }
        } catch (ClassCastException ccE) {
            LOGGER.fine(fileNode.toString()+" is not a FileStat object.");
        }
        return;
    }

    /**
     * Descends through the dirTree searching for files which are
     * out of sync and returns an ArrayList of files to be 
     * synchronised.
     *
     * @param path This is the path you want to start searching from.
     * @param depth Number of levels deep you want to go.
     *              0 for Just resyncing the current directory.
     *              -1 for everything below this point.
     */
    public ArrayList<String> getUnSyncedPaths(String path, int depth, int direction) {
        return getUnSyncedPaths(null, search_tree(path), depth, direction);
    }

    /**
     * Descends through the dirTree searching for files which are
     * out of sync and returns an ArrayList of files to be 
     * synchronised.
     *
     * @param node This is the node you want to start searching from.
     * @param depth Number of levels deep you want to go.
     *              0 for Just resyncing the current directory.
     *              -1 for everything below this point.
     */
    private ArrayList<String> getUnSyncedPaths(ArrayList<String> unsyncedPaths, DefaultMutableTreeNode node, int depth, int direction) {
        if (unsyncedPaths == null) {
            unsyncedPaths = new ArrayList<String>();
        }
        Enumeration en = node.children();
        //Iterate through the directory contents
        while (en.hasMoreElements()) {
            DefaultMutableTreeNode gotNode = (DefaultMutableTreeNode) en.nextElement();
            FileStat gotUserObject = (FileStat) gotNode.getUserObject();
            //Build file path from tree ancestors
            TreeNode[] nodeArray = gotNode.getPath();
            StringBuilder pathBuilder = new StringBuilder();
            for ( TreeNode fileNode : nodeArray ) {
                String pathSection = fileNode.toString();
                if (pathSection != "/") {
                    pathBuilder.append("/"+pathSection);
                }
            }
            String path = pathBuilder.toString();
            if (gotUserObject.getType().contains("dir")) {
                if ( depth != 0 ) {
                    //Go deeper. Then add everything from down there.
                    unsyncedPaths.addAll(getUnSyncedPaths(unsyncedPaths,gotNode,depth-1,direction));
                }
                else {
                    //Don't need to copy directories so go on to the next file.
                    continue;
                }
            }
            else {
                int cacheStatus = gotUserObject.getCacheStatus();
                if (cacheStatus == direction) {
                    //Add file to list of unsynced files
                    unsyncedPaths.add(path);
                    LOGGER.finer("Adding "+path+" to unsyncedPaths list");
                }
            }
        }
        return unsyncedPaths;
    }

    /**
     * Uses Logger to recursively print cached directory tree structure 
     * to terminal for debugging.
     *
     * @param node This is the node you want to start printing from. If node is
     * null, print from root.
     * @param treeStringBuilder StringBuilder which creates/holds the dirTree output
     * @param prepender How you want to display the depth of the tree (e.g. "\t")
     * @param depth Set this to 0, it increments itself when going deeper into the tree
     */
    private void printTree(DefaultMutableTreeNode node, StringBuilder treeStringBuilder, String prepender, int depth) {
        if (node == null) {
            node = root;
        }
        if (depth < 0) {
            depth = 0;
        }
        //This appends to the StringBuilder with [depth] copies of the prepender string
        String prepending = new String(new char[depth]).replace("\0",prepender);
        treeStringBuilder.append(prepending + node.toString() + "\n");
        //Each time we get here, the tree will be at a deeper level so increment depth
        depth++;
        Enumeration en = node.children();
        while (en.hasMoreElements()) {
            DefaultMutableTreeNode gotNode = (DefaultMutableTreeNode) en.nextElement();
            printTree(gotNode, treeStringBuilder, prepender, depth);
        }
    }

    /**
     * Uses Logger to recursively print cached directory tree structure 
     * to terminal for debugging.
     *
     * @param path This is the directory you want to start printing from. If path is
     * null, print from root.
     * @param prepender How you want to display the depth of the tree (e.g. "\t")
     * @param depth Set this to 0, it increments itself when going deeper into the tree
     */
    protected void printTree(String path, String prepender, int depth) {
        if (path == null) { path = "/"; }
        StringBuilder treeStringBuilder = new StringBuilder();
        printTree(search_tree(path),treeStringBuilder,prepender,depth);
        LOGGER.info("Directory tree:\n"+treeStringBuilder.toString());
        return;
    }

}
