bin_PROGRAMS = bbfs
bbfs_SOURCES = bbfs.c log.c log.h  params.h
AM_CFLAGS = @FUSE_CFLAGS@ -I${JAVA_HOME}/include -I${JAVA_HOME}/include/linux
LDADD = @FUSE_LIBS@ -L${LD_LIBRARY_PATH} -ljvm
