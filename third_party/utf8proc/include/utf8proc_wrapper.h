// C wrapper of the utf8proc library

#pragma once

#ifdef __cplusplus
extern "C" {
#endif

//! Returns whether or not the string is valid UTF8
int utf8proc_is_valid(const char *s, size_t len);

//! Returns the position (in bytes) of the next grapheme cluster
size_t utf8proc_next_grapheme_cluster(const char *s, size_t len, size_t pos);

//! Returns the position (in bytes) of the previous grapheme cluster
size_t utf8proc_prev_grapheme_cluster(const char *s, size_t len, size_t pos);

//! Returns the render width [0, 1 or 2] of the grapheme cluster as the specified position
size_t utf8proc_render_width(const char *s, size_t len, size_t pos);

#ifdef __cplusplus
};
#endif
