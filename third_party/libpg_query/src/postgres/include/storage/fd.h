/*-------------------------------------------------------------------------
 *
 * fd.h
 *	  Virtual file descriptor definitions.
 *
 *
 * Portions Copyright (c) 1996-2015, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/storage/fd.h
 *
 *-------------------------------------------------------------------------
 */

/*
 * calls:
 *
 *	File {Close, Read, Write, Seek, Tell, Sync}
 *	{Path Name Open, Allocate, Free} File
 *
 * These are NOT JUST RENAMINGS OF THE UNIX ROUTINES.
 * Use them for all file activity...
 *
 *	File fd;
 *	fd = PathNameOpenFile("foo", O_RDONLY, 0600);
 *
 *	AllocateFile();
 *	FreeFile();
 *
 * Use AllocateFile, not fopen, if you need a stdio file (FILE*); then
 * use FreeFile, not fclose, to close it.  AVOID using stdio for files
 * that you intend to hold open for any length of time, since there is
 * no way for them to share kernel file descriptors with other files.
 *
 * Likewise, use AllocateDir/FreeDir, not opendir/closedir, to allocate
 * open directories (DIR*), and OpenTransientFile/CloseTransient File for an
 * unbuffered file descriptor.
 */
#ifndef FD_H
#define FD_H

#include <dirent.h>


/*
 * FileSeek uses the standard UNIX lseek(2) flags.
 */

typedef char *FileName;

typedef int File;


/* GUC parameter */
extern int	max_files_per_process;

/*
 * This is private to fd.c, but exported for save/restore_backend_variables()
 */
extern int	max_safe_fds;


/*
 * prototypes for functions in fd.c
 */

/* Operations on virtual Files --- equivalent to Unix kernel file ops */
extern File PathNameOpenFile(FileName fileName, int fileFlags, int fileMode);
extern File OpenTemporaryFile(bool interXact);
extern void FileClose(File file);
extern int	FilePrefetch(File file, off_t offset, int amount);
extern int	FileRead(File file, char *buffer, int amount);
extern int	FileWrite(File file, char *buffer, int amount);
extern int	FileSync(File file);
extern off_t FileSeek(File file, off_t offset, int whence);
extern int	FileTruncate(File file, off_t offset);
extern char *FilePathName(File file);

/* Operations that allow use of regular stdio --- USE WITH CAUTION */
extern FILE *AllocateFile(const char *name, const char *mode);
extern int	FreeFile(FILE *file);

/* Operations that allow use of pipe streams (popen/pclose) */
extern FILE *OpenPipeStream(const char *command, const char *mode);
extern int	ClosePipeStream(FILE *file);

/* Operations to allow use of the <dirent.h> library routines */
extern DIR *AllocateDir(const char *dirname);
extern struct dirent *ReadDir(DIR *dir, const char *dirname);
extern int	FreeDir(DIR *dir);

/* Operations to allow use of a plain kernel FD, with automatic cleanup */
extern int	OpenTransientFile(FileName fileName, int fileFlags, int fileMode);
extern int	CloseTransientFile(int fd);

/* If you've really really gotta have a plain kernel FD, use this */
extern int	BasicOpenFile(FileName fileName, int fileFlags, int fileMode);

/* Miscellaneous support routines */
extern void InitFileAccess(void);
extern void set_max_safe_fds(void);
extern void closeAllVfds(void);
extern void SetTempTablespaces(Oid *tableSpaces, int numSpaces);
extern bool TempTablespacesAreSet(void);
extern Oid	GetNextTempTableSpace(void);
extern void AtEOXact_Files(void);
extern void AtEOSubXact_Files(bool isCommit, SubTransactionId mySubid,
				  SubTransactionId parentSubid);
extern void RemovePgTempFiles(void);

extern int	pg_fsync(int fd);
extern int	pg_fsync_no_writethrough(int fd);
extern int	pg_fsync_writethrough(int fd);
extern int	pg_fdatasync(int fd);
extern int	pg_flush_data(int fd, off_t offset, off_t amount);
extern void fsync_fname(const char *fname, bool isdir);
extern int	durable_rename(const char *oldfile, const char *newfile, int loglevel);
extern int	durable_link_or_rename(const char *oldfile, const char *newfile, int loglevel);
extern void SyncDataDirectory(void);

/* Filename components for OpenTemporaryFile */
#define PG_TEMP_FILES_DIR "pgsql_tmp"
#define PG_TEMP_FILE_PREFIX "pgsql_tmp"

#endif   /* FD_H */
