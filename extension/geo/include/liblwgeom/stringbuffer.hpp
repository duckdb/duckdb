/**********************************************************************
 *
 * PostGIS - Spatial Types for PostgreSQL
 * http://postgis.net
 *
 * PostGIS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * PostGIS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with PostGIS.  If not, see <http://www.gnu.org/licenses/>.
 *
 **********************************************************************
 *
 * Copyright 2002 Thamer Alharbash
 * Copyright 2009 Paul Ramsey <pramsey@cleverelephant.ca>
 *
 **********************************************************************/

#pragma once
#include "duckdb.hpp"
#include "liblwgeom/liblwgeom_internal.hpp"

#include <cstring>
#include <stdarg.h>

namespace duckdb {
#ifndef _STRINGBUFFER_H
#define _STRINGBUFFER_H 1

#define STRINGBUFFER_STARTSIZE 128

typedef struct {
	size_t capacity;
	char *str_end;
	char *str_start;
} stringbuffer_t;

extern stringbuffer_t *stringbuffer_create_with_size(size_t size);
extern stringbuffer_t *stringbuffer_create(void);
extern void stringbuffer_release(stringbuffer_t *s);
extern int stringbuffer_aprintf(stringbuffer_t *sb, const char *fmt, ...);
extern void stringbuffer_destroy(stringbuffer_t *sb);
extern const char *stringbuffer_getstring(stringbuffer_t *sb);
extern char *stringbuffer_getstringcopy(stringbuffer_t *sb);
extern int stringbuffer_getlength(stringbuffer_t *sb);
extern char stringbuffer_lastchar(stringbuffer_t *s);

/**
 * If necessary, expand the stringbuffer_t internal buffer to accommodate the
 * specified additional size.
 */
static inline void stringbuffer_makeroom(stringbuffer_t *s, size_t size_to_add) {
	size_t current_size = (s->str_end - s->str_start);
	size_t capacity = s->capacity;
	size_t required_size = current_size + size_to_add;

	while (capacity < required_size)
		capacity *= 2;

	if (capacity > s->capacity) {
		s->str_start = (char *)lwrealloc(s->str_start, capacity);
		s->capacity = capacity;
		s->str_end = s->str_start + current_size;
	}
}

/**
 * Append the specified string to the stringbuffer_t using known length
 */
inline static void stringbuffer_append_len(stringbuffer_t *s, const char *a, size_t alen) {
	int alen0 = alen + 1; /* Length including null terminator */
	stringbuffer_makeroom(s, alen0);
	memcpy(s->str_end, a, alen0);
	s->str_end += alen;
}

inline static void stringbuffer_append_double(stringbuffer_t *s, double d, int precision) {
	stringbuffer_makeroom(s, OUT_MAX_BYTES_DOUBLE);
	s->str_end += lwprint_double(d, precision, s->str_end);
}

#endif /* _STRINGBUFFER_H */
} // namespace duckdb
