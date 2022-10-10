#include "printbuf.hpp"

#include <cstring>

namespace json {

/**
 * Extend the buffer p so it has a size of at least min_size.
 *
 * If the current size is large enough, nothing is changed.
 *
 * Note: this does not check the available space!  The caller
 *  is responsible for performing those calculations.
 */
static int printbuf_extend(struct printbuf *p, int min_size) {
	char *t;
	int new_size;

	if (p->size >= min_size)
		return 0;
	/* Prevent signed integer overflows with large buffers. */
	if (min_size > INT_MAX - 8)
		return -1;
	if (p->size > INT_MAX / 2)
		new_size = min_size + 8;
	else {
		new_size = p->size * 2;
		if (new_size < min_size + 8)
			new_size = min_size + 8;
	}
	if (!(t = (char *)realloc(p->buf, new_size)))
		return -1;
	p->size = new_size;
	p->buf = t;
	return 0;
}

int printbuf_memappend(struct printbuf *p, const char *buf, int size) {
	/* Prevent signed integer overflows with large buffers. */
	if (size > INT_MAX - p->bpos - 1)
		return -1;
	if (p->size <= p->bpos + size + 1) {
		if (printbuf_extend(p, p->bpos + size + 1) < 0)
			return -1;
	}
	memcpy(p->buf + p->bpos, buf, size);
	p->bpos += size;
	p->buf[p->bpos] = '\0';
	return size;
}

struct printbuf *printbuf_new(void) {
	struct printbuf *p;

	p = (struct printbuf *)calloc(1, sizeof(struct printbuf));
	if (!p)
		return NULL;
	p->size = 32;
	p->bpos = 0;
	if (!(p->buf = (char *)malloc(p->size))) {
		free(p);
		return NULL;
	}
	p->buf[0] = '\0';
	return p;
}

void printbuf_reset(struct printbuf *p) {
	p->buf[0] = '\0';
	p->bpos = 0;
}

void printbuf_free(struct printbuf *p) {
	if (p) {
		free(p->buf);
		free(p);
	}
}

int printbuf_memset(struct printbuf *pb, int offset, int charvalue, int len) {
	int size_needed;

	if (offset == -1)
		offset = pb->bpos;
	/* Prevent signed integer overflows with large buffers. */
	if (len > INT_MAX - offset)
		return -1;
	size_needed = offset + len;
	if (pb->size < size_needed) {
		if (printbuf_extend(pb, size_needed) < 0)
			return -1;
	}

	memset(pb->buf + offset, charvalue, len);
	if (pb->bpos < size_needed)
		pb->bpos = size_needed;

	return 0;
}

} // namespace json
