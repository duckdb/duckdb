/* src/include/port/darwin.h */

#define __darwin__	1

#if HAVE_DECL_F_FULLFSYNC		/* not present before OS X 10.3 */
#define HAVE_FSYNC_WRITETHROUGH

#endif
