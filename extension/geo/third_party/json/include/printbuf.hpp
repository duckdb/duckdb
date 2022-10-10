#pragma one

#include <cstring>
#include <limits.h>
#include <stdlib.h> // pulls in declaration of malloc, free
#include <string.h> // pulls in declaration for strlen.

namespace json {

struct printbuf {
	char *buf;
	int bpos;
	int size;
};
typedef struct printbuf printbuf;

/* As an optimization, printbuf_memappend_fast() is defined as a macro
 * that handles copying data if the buffer is large enough; otherwise
 * it invokes printbuf_memappend() which performs the heavy
 * lifting of realloc()ing the buffer and copying data.
 *
 * Your code should not use printbuf_memappend() directly unless it
 * checks the return code. Use printbuf_memappend_fast() instead.
 */
extern int printbuf_memappend(struct printbuf *p, const char *buf, int size);

#define printbuf_memappend_fast(p, bufptr, bufsize)                                                                    \
	do {                                                                                                               \
		if ((p->size - p->bpos) > bufsize) {                                                                           \
			memcpy(p->buf + p->bpos, (bufptr), bufsize);                                                               \
			p->bpos += bufsize;                                                                                        \
			p->buf[p->bpos] = '\0';                                                                                    \
		} else {                                                                                                       \
			printbuf_memappend(p, (bufptr), bufsize);                                                                  \
		}                                                                                                              \
	} while (0)

#define printbuf_length(p) ((p)->bpos)

/**
 * Results in a compile error if the argument is not a string literal.
 */
#define _printbuf_check_literal(mystr) ("" mystr)

/**
 * This is an optimization wrapper around printbuf_memappend() that is useful
 * for appending string literals. Since the size of string constants is known
 * at compile time, using this macro can avoid a costly strlen() call. This is
 * especially helpful when a constant string must be appended many times. If
 * you got here because of a compilation error caused by passing something
 * other than a string literal, use printbuf_memappend_fast() in conjunction
 * with strlen().
 *
 * See also:
 *   printbuf_memappend_fast()
 *   printbuf_memappend()
 *   sprintbuf()
 */
#define printbuf_strappend(pb, str) printbuf_memappend((pb), _printbuf_check_literal(str), sizeof(str) - 1)

extern struct printbuf *printbuf_new(void);

extern void printbuf_reset(struct printbuf *p);

extern void printbuf_free(struct printbuf *p);

/**
 * Set len bytes of the buffer to charvalue, starting at offset offset.
 * Similar to calling memset(x, charvalue, len);
 *
 * The memory allocated for the buffer is extended as necessary.
 *
 * If offset is -1, this starts at the end of the current data in the buffer.
 */
extern int printbuf_memset(struct printbuf *pb, int offset, int charvalue, int len);

} // namespace json
