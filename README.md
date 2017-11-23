# daaas-grid-fuse
## Dependencies:

```
java-1.8.0-openjdk-devel
maven
fuse-libs
fuse-devel
fuse
A C compiler
A GridFTP setup with at least two transfer nodes (One local and one remote)
```

## Setup

### Config
Create config files and edit to taste:
```
jglobus/resources/config.properties
jglobus/resources/logging.properties
```
Edit fuse.conf to enable allow_other (/etc/fuse.conf).

Configure both JAVA_HOME and LD_LIBRARY_PATH environment variables.

### Build/Compile
```
cd jglobus/  
mvn -q compile assembly:single  
cd fuse  
./configure
make
```

### Start grid-fuse
```
cd fuse/example/
../src/bbfs -d -o "big_writes,allow_other" [localrootdir] [mountdir]
```

###### Quick test in another session
```
ls [mountdir]/
```

###### When you're finished:
```
fusermount -u [mountdir]/
```

