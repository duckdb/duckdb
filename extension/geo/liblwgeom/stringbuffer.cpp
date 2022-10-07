#include "liblwgeom/stringbuffer.hpp"

#include <cstring>

namespace duckdb {

static void stringbuffer_init_with_size(stringbuffer_t *s, size_t size) {
	s->str_start = (char *)lwalloc(size);
	s->str_end = s->str_start;
	s->capacity = size;
	memset(s->str_start, 0, size);
}

/**
 * Allocate a new stringbuffer_t. Use stringbuffer_destroy to free.
 */
stringbuffer_t *stringbuffer_create_with_size(size_t size) {
	stringbuffer_t *s;

	s = (stringbuffer_t *)lwalloc(sizeof(stringbuffer_t));
	stringbuffer_init_with_size(s, size);
	return s;
}

/**
 * Return the last character in the buffer.
 */
char stringbuffer_lastchar(stringbuffer_t *s) {
	if (s->str_end == s->str_start)
		return 0;

	return *(s->str_end - 1);
}

/**
 * Allocate a new stringbuffer_t. Use stringbuffer_destroy to free.
 */
stringbuffer_t *stringbuffer_create(void) {
	return stringbuffer_create_with_size(STRINGBUFFER_STARTSIZE);
}

void stringbuffer_release(stringbuffer_t *s) {
	if (s->str_start)
		lwfree(s->str_start);
}

/**
 * Returns a reference to the internal string being managed by
 * the stringbuffer. The current string will be null-terminated
 * within the internal string.
 */
const char *stringbuffer_getstring(stringbuffer_t *s) {
	return s->str_start;
}

/**
 * Returns a newly allocated string large enough to contain the
 * current state of the string. Caller is responsible for
 * freeing the return value.
 */
char *stringbuffer_getstringcopy(stringbuffer_t *s) {
	size_t size = (s->str_end - s->str_start) + 1;
	char *str = (char *)lwalloc(size);
	memcpy(str, s->str_start, size);
	str[size - 1] = '\0';
	return str;
}

/**
 * Free the stringbuffer_t and all memory managed within it.
 */
void stringbuffer_destroy(stringbuffer_t *s) {
	stringbuffer_release(s);
	if (s)
		lwfree(s);
}

/**
 * Appends a formatted string to the current string buffer,
 * using the format and argument list provided. Returns -1 on error,
 * check errno for reasons, documented in the printf man page.
 */
static int stringbuffer_avprintf(stringbuffer_t *s, const char *fmt, va_list ap) {
	int maxlen = (s->capacity - (s->str_end - s->str_start));
	int len = 0; /* Length of the output */
	va_list ap2;

	/* Make a copy of the variadic arguments, in case we need to print twice */
	/* Print to our buffer */
	va_copy(ap2, ap);
	len = vsnprintf(s->str_end, maxlen, fmt, ap2);
	va_end(ap2);

	/* Propogate errors up */
	if (len < 0)
#if defined(__MINGW64_VERSION_MAJOR)
		len = _vscprintf(
		    fmt,
		    ap2); /**Assume windows flaky vsnprintf that returns -1 if initial buffer to small and add more space **/
#else
		return len;
#endif

	/* We didn't have enough space! */
	/* Either Unix vsnprint returned write length larger than our buffer */
	/*     or Windows vsnprintf returned an error code. */
	if (len >= maxlen) {
		stringbuffer_makeroom(s, len + 1);
		maxlen = (s->capacity - (s->str_end - s->str_start));

		/* Try to print a second time */
		len = vsnprintf(s->str_end, maxlen, fmt, ap);

		/* Printing error? Error! */
		if (len < 0)
			return len;
		/* Too long still? Error! */
		if (len >= maxlen)
			return -1;
	}

	/* Move end pointer forward and return. */
	s->str_end += len;
	return len;
}

/**
 * Appends a formatted string to the current string buffer,
 * using the format and argument list provided.
 * Returns -1 on error, check errno for reasons,
 * as documented in the printf man page.
 */
int stringbuffer_aprintf(stringbuffer_t *s, const char *fmt, ...) {
	int r;
	va_list ap;
	va_start(ap, fmt);
	r = stringbuffer_avprintf(s, fmt, ap);
	va_end(ap);
	return r;
}

/**
 * Returns the length of the current string, not including the
 * null terminator (same behavior as strlen()).
 */
int stringbuffer_getlength(stringbuffer_t *s) {
	return (s->str_end - s->str_start);
}

} // namespace duckdb