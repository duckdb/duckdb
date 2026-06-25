/*
 * io_shim — syscall interposition library for the I/O-metrics test harness.
 *
 * Counts the real bytes that the process reads from / writes to a set of
 * target files, observed at the libc read/write/pread/pwrite boundary. This
 * is "ground truth" I/O, independent of DuckDB's own FileSystem instrumentation.
 *
 * A file descriptor is counted when it was opened with a path that starts with
 * one of the prefixes in IOSHIM_INCLUDE (comma-separated). This naturally
 * covers a DuckDB database file together with its ".wal" and temp files when
 * the prefix is the database path.
 *
 * On exit the totals are written as JSON to IOSHIM_OUT (or stderr).
 *
 * Build (macOS):  cc -O2 -dynamiclib -o io_shim.dylib io_shim.c
 * Build (Linux):  cc -O2 -shared -fPIC -o io_shim.so io_shim.c -ldl
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <sys/types.h>

#define MAX_FDS 65536
#define MAX_PREFIX 16

static char g_prefix[MAX_PREFIX][4096];
static int  g_nprefix = 0;
static unsigned char g_counted[MAX_FDS];     /* 1 if fd refers to a target file */
static long long g_read = 0, g_write = 0;

__attribute__((constructor))
static void ioshim_init(void) {
	const char *inc = getenv("IOSHIM_INCLUDE");
	if (!inc) {
		return;
	}
	char buf[65536];
	strncpy(buf, inc, sizeof(buf) - 1);
	buf[sizeof(buf) - 1] = '\0';
	char *save = NULL;
	for (char *tok = strtok_r(buf, ",", &save); tok && g_nprefix < MAX_PREFIX;
	     tok = strtok_r(NULL, ",", &save)) {
		strncpy(g_prefix[g_nprefix], tok, 4095);
		g_prefix[g_nprefix][4095] = '\0';
		g_nprefix++;
	}
}

static int path_is_target(const char *path) {
	if (!path) {
		return 0;
	}
	for (int i = 0; i < g_nprefix; i++) {
		size_t n = strlen(g_prefix[i]);
		if (strncmp(path, g_prefix[i], n) == 0) {
			return 1;
		}
	}
	return 0;
}

static void mark_fd(int fd, const char *path) {
	if (fd < 0 || fd >= MAX_FDS) {
		return;
	}
	g_counted[fd] = path_is_target(path) ? 1 : 0;
}

static inline void add_read(int fd, ssize_t r) {
	if (r > 0 && fd >= 0 && fd < MAX_FDS && g_counted[fd]) {
		g_read += r;
	}
}
static inline void add_write(int fd, ssize_t r) {
	if (r > 0 && fd >= 0 && fd < MAX_FDS && g_counted[fd]) {
		g_write += r;
	}
}

__attribute__((destructor))
static void ioshim_dump(void) {
	const char *out = getenv("IOSHIM_OUT");
	FILE *f = out ? fopen(out, "w") : stderr;
	if (!f) {
		return;
	}
	fprintf(f, "{\"os_bytes_read\": %lld, \"os_bytes_written\": %lld}\n", g_read, g_write);
	if (out) {
		fclose(f);
	}
}

/* ----------------------------------------------------------------------- */
#if defined(__APPLE__)
/* macOS: function interposition via the __interpose section. We call the real
 * symbols directly (the loader resolves them to libsystem, not back to us). */

static int my_open(const char *path, int flags, ...) {
	extern int open(const char *, int, ...);
	mode_t mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = (mode_t)va_arg(ap, int);
		va_end(ap);
	}
	int fd = open(path, flags, mode);
	mark_fd(fd, path);
	return fd;
}
static int my_openat(int dirfd, const char *path, int flags, ...) {
	extern int openat(int, const char *, int, ...);
	mode_t mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = (mode_t)va_arg(ap, int);
		va_end(ap);
	}
	int fd = openat(dirfd, path, flags, mode);
	mark_fd(fd, path);
	return fd;
}
static int my_close(int fd) {
	extern int close(int);
	if (fd >= 0 && fd < MAX_FDS) {
		g_counted[fd] = 0;
	}
	return close(fd);
}
static ssize_t my_read(int fd, void *b, size_t n) {
	extern ssize_t read(int, void *, size_t);
	ssize_t r = read(fd, b, n);
	add_read(fd, r);
	return r;
}
static ssize_t my_write(int fd, const void *b, size_t n) {
	extern ssize_t write(int, const void *, size_t);
	ssize_t r = write(fd, b, n);
	add_write(fd, r);
	return r;
}
static ssize_t my_pread(int fd, void *b, size_t n, off_t o) {
	extern ssize_t pread(int, void *, size_t, off_t);
	ssize_t r = pread(fd, b, n, o);
	add_read(fd, r);
	return r;
}
static ssize_t my_pwrite(int fd, const void *b, size_t n, off_t o) {
	extern ssize_t pwrite(int, const void *, size_t, off_t);
	ssize_t r = pwrite(fd, b, n, o);
	add_write(fd, r);
	return r;
}

#define INTERPOSE(newf, oldf)                                                                                          \
	__attribute__((used)) static struct {                                                                            \
		const void *n;                                                                                               \
		const void *o;                                                                                               \
	} _ip_##oldf __attribute__((section("__DATA,__interpose"))) = {(const void *)(uintptr_t)&newf,                   \
	                                                               (const void *)(uintptr_t)&oldf};
INTERPOSE(my_open, open)
INTERPOSE(my_openat, openat)
INTERPOSE(my_close, close)
INTERPOSE(my_read, read)
INTERPOSE(my_write, write)
INTERPOSE(my_pread, pread)
INTERPOSE(my_pwrite, pwrite)

/* ----------------------------------------------------------------------- */
#else
/* Linux/glibc: LD_PRELOAD with dlsym(RTLD_NEXT). We export the symbols, so we
 * must look up the real ones at runtime. */
#include <dlfcn.h>

#define REAL(name) real_##name

static int (*REAL(open))(const char *, int, ...) = NULL;
static int (*REAL(open64))(const char *, int, ...) = NULL;
static int (*REAL(openat))(int, const char *, int, ...) = NULL;
static int (*REAL(close))(int) = NULL;
static ssize_t (*REAL(read))(int, void *, size_t) = NULL;
static ssize_t (*REAL(write))(int, const void *, size_t) = NULL;
static ssize_t (*REAL(pread))(int, void *, size_t, off_t) = NULL;
static ssize_t (*REAL(pwrite))(int, const void *, size_t, off_t) = NULL;
static ssize_t (*REAL(pread64))(int, void *, size_t, off_t) = NULL;
static ssize_t (*REAL(pwrite64))(int, const void *, size_t, off_t) = NULL;

#define RESOLVE(name)                                                                                                  \
	do {                                                                                                             \
		if (!REAL(name)) {                                                                                          \
			REAL(name) = dlsym(RTLD_NEXT, #name);                                                                 \
		}                                                                                                          \
	} while (0)

int open(const char *path, int flags, ...) {
	RESOLVE(open);
	mode_t mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = (mode_t)va_arg(ap, int);
		va_end(ap);
	}
	int fd = REAL(open)(path, flags, mode);
	mark_fd(fd, path);
	return fd;
}
int open64(const char *path, int flags, ...) {
	RESOLVE(open64);
	mode_t mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = (mode_t)va_arg(ap, int);
		va_end(ap);
	}
	int fd = REAL(open64)(path, flags, mode);
	mark_fd(fd, path);
	return fd;
}
int openat(int dirfd, const char *path, int flags, ...) {
	RESOLVE(openat);
	mode_t mode = 0;
	if (flags & O_CREAT) {
		va_list ap;
		va_start(ap, flags);
		mode = (mode_t)va_arg(ap, int);
		va_end(ap);
	}
	int fd = REAL(openat)(dirfd, path, flags, mode);
	mark_fd(fd, path);
	return fd;
}
int close(int fd) {
	RESOLVE(close);
	if (fd >= 0 && fd < MAX_FDS) {
		g_counted[fd] = 0;
	}
	return REAL(close)(fd);
}
ssize_t read(int fd, void *b, size_t n) {
	RESOLVE(read);
	ssize_t r = REAL(read)(fd, b, n);
	add_read(fd, r);
	return r;
}
ssize_t write(int fd, const void *b, size_t n) {
	RESOLVE(write);
	ssize_t r = REAL(write)(fd, b, n);
	add_write(fd, r);
	return r;
}
ssize_t pread(int fd, void *b, size_t n, off_t o) {
	RESOLVE(pread);
	ssize_t r = REAL(pread)(fd, b, n, o);
	add_read(fd, r);
	return r;
}
ssize_t pwrite(int fd, const void *b, size_t n, off_t o) {
	RESOLVE(pwrite);
	ssize_t r = REAL(pwrite)(fd, b, n, o);
	add_write(fd, r);
	return r;
}
ssize_t pread64(int fd, void *b, size_t n, off_t o) {
	RESOLVE(pread64);
	ssize_t r = REAL(pread64)(fd, b, n, o);
	add_read(fd, r);
	return r;
}
ssize_t pwrite64(int fd, const void *b, size_t n, off_t o) {
	RESOLVE(pwrite64);
	ssize_t r = REAL(pwrite64)(fd, b, n, o);
	add_write(fd, r);
	return r;
}
#endif
