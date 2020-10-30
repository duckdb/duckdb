#include "duckdb/common/strnstrn.hpp"

#include <cstring>

namespace duckdb {
// inspired by FreeBSD strnstr
// this loops through s and compares the characters with the first character of find. If they match, strncmp is called.
char *strnstrn(const char *s, const char *find, idx_t s_len, idx_t find_len) {
	char c, sc;
	if (find_len < 1) {
		return (char *)s;
	}
	c = *find++;
	find_len--;
	do {
		do {
			if (s_len-- < 1) {
				return nullptr;
			}
			sc = *s++;
		} while (sc != c);
		if (find_len > s_len) {
			return nullptr;
		}
	} while (strncmp(s, find, find_len) != 0);
	s--;
	return (char *)s;
}

} // namespace duckdb
