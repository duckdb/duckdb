#include "json_tokener.hpp"

#include "config.hpp"
#include "json_object.hpp"
#include "json_util.hpp"

#include <cstdlib>
#include <cstring>

namespace json {

#if !HAVE_STRNCASECMP && defined(_MSC_VER)
/* MSC has the version as _strnicmp */
#define strncasecmp _strnicmp
#elif !HAVE_STRNCASECMP
#error You do not have strncasecmp on your system.
#endif /* HAVE_STRNCASECMP */

static const char *json_tokener_errors[] = {"success",
                                            "continue",
                                            "nesting too deep",
                                            "unexpected end of data",
                                            "unexpected character",
                                            "null expected",
                                            "boolean expected",
                                            "number expected",
                                            "array value separator ',' expected",
                                            "quoted object property name expected",
                                            "object property name separator ':' expected",
                                            "object value separator ',' expected",
                                            "invalid string sequence",
                                            "expected comment",
                                            "invalid utf-8 string",
                                            "buffer size overflow"};

#define jt_hexdigit(x) (((x) <= '9') ? (x) - '0' : ((x)&7) + 9)

/* Stuff for decoding unicode sequences */
#define IS_HIGH_SURROGATE(uc)         (((uc)&0xFC00) == 0xD800)
#define IS_LOW_SURROGATE(uc)          (((uc)&0xFC00) == 0xDC00)
#define DECODE_SURROGATE_PAIR(hi, lo) ((((hi)&0x3FF) << 10) + ((lo)&0x3FF) + 0x10000)
static unsigned char utf8_replacement_char[3] = {0xEF, 0xBF, 0xBD};

static const char json_null_str[] = "null";
static const int json_null_str_len = sizeof(json_null_str) - 1;
static const char json_inf_str[] = "Infinity";
/* Swapped case "Infinity" to avoid need to call tolower() on input chars: */
static const char json_inf_str_invert[] = "iNFINITY";
static const unsigned int json_inf_str_len = sizeof(json_inf_str) - 1;
static const char json_nan_str[] = "NaN";
static const int json_nan_str_len = sizeof(json_nan_str) - 1;
static const char json_true_str[] = "true";
static const int json_true_str_len = sizeof(json_true_str) - 1;
static const char json_false_str[] = "false";
static const int json_false_str_len = sizeof(json_false_str) - 1;

/* The following helper functions are used to speed up parsing. They
 * are faster than their ctype counterparts because they assume that
 * the input is in ASCII and that the locale is set to "C". The
 * compiler will also inline these functions, providing an additional
 * speedup by saving on function calls.
 */
static inline int is_ws_char(char c) {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r';
}

static inline int is_hex_char(char c) {
	return (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') || (c >= 'a' && c <= 'f');
}

struct json_tokener *json_tokener_new(void) {
	return json_tokener_new_ex(JSON_TOKENER_DEFAULT_DEPTH);
}

struct json_tokener *json_tokener_new_ex(int depth) {
	struct json_tokener *tok;

	tok = (struct json_tokener *)calloc(1, sizeof(struct json_tokener));
	if (!tok)
		return NULL;
	tok->stack = (struct json_tokener_srec *)calloc(depth, sizeof(struct json_tokener_srec));
	if (!tok->stack) {
		free(tok);
		return NULL;
	}
	tok->pb = printbuf_new();
	if (!tok->pb) {
		free(tok->stack);
		free(tok);
		return NULL;
	}
	tok->max_depth = depth;
	json_tokener_reset(tok);
	return tok;
}

static json_bool json_tokener_validate_utf8(const char c, unsigned int *nBytes) {
	unsigned char chr = c;
	if (*nBytes == 0) {
		if (chr >= 0x80) {
			if ((chr & 0xe0) == 0xc0)
				*nBytes = 1;
			else if ((chr & 0xf0) == 0xe0)
				*nBytes = 2;
			else if ((chr & 0xf8) == 0xf0)
				*nBytes = 3;
			else
				return 0;
		}
	} else {
		if ((chr & 0xC0) != 0x80)
			return 0;
		(*nBytes)--;
	}
	return 1;
}

static void json_tokener_reset_level(struct json_tokener *tok, int depth) {
	tok->stack[depth].state = json_tokener_state_eatws;
	tok->stack[depth].saved_state = json_tokener_state_start;
	json_object_put(tok->stack[depth].current);
	tok->stack[depth].current = NULL;
	free(tok->stack[depth].obj_field_name);
	tok->stack[depth].obj_field_name = NULL;
}

void json_tokener_reset(struct json_tokener *tok) {
	int i;
	if (!tok)
		return;

	for (i = tok->depth; i >= 0; i--)
		json_tokener_reset_level(tok, i);
	tok->depth = 0;
	tok->err = json_tokener_success;
}

static int json_tokener_parse_double(const char *buf, int len, double *retval) {
	char *end;
	*retval = strtod(buf, &end);
	if (buf + len == end)
		return 0; // It worked
	return 1;
}

#define state          tok->stack[tok->depth].state
#define saved_state    tok->stack[tok->depth].saved_state
#define current        tok->stack[tok->depth].current
#define obj_field_name tok->stack[tok->depth].obj_field_name

/* PEEK_CHAR(dest, tok) macro:
 *   Peeks at the current char and stores it in dest.
 *   Returns 1 on success, sets tok->err and returns 0 if no more chars.
 *   Implicit inputs:  str, len, nBytesp vars
 */
#define PEEK_CHAR(dest, tok)                                                                                           \
	(((tok)->char_offset == len)                                                                                       \
	     ? (((tok)->depth == 0 && state == json_tokener_state_eatws && saved_state == json_tokener_state_finish)       \
	            ? (((tok)->err = json_tokener_success), 0)                                                             \
	            : (((tok)->err = json_tokener_continue), 0))                                                           \
	     : (((tok->flags & JSON_TOKENER_VALIDATE_UTF8) && (!json_tokener_validate_utf8(*str, nBytesp)))                \
	            ? ((tok->err = json_tokener_error_parse_utf8_string), 0)                                               \
	            : (((dest) = *str), 1)))

/* ADVANCE_CHAR() macro:
 *   Increments str & tok->char_offset.
 *   For convenience of existing conditionals, returns the old value of c (0 on eof)
 *   Implicit inputs:  c var
 */
#define ADVANCE_CHAR(str, tok) (++(str), ((tok)->char_offset)++, c)

struct json_object *json_tokener_parse_ex(struct json_tokener *tok, const char *str, int len) {
	struct json_object *obj = NULL;
	char c = '\1';
	unsigned int nBytes = 0;
	unsigned int *nBytesp = &nBytes;

#ifdef HAVE_USELOCALE
	locale_t oldlocale = uselocale(NULL);
	locale_t newloc;
#elif defined(HAVE_SETLOCALE)
	char *oldlocale = NULL;
#endif

	tok->char_offset = 0;
	tok->err = json_tokener_success;

	/* this interface is presently not 64-bit clean due to the int len argument
	 * and the internal printbuf interface that takes 32-bit int len arguments
	 * so the function limits the maximum string size to INT32_MAX (2GB).
	 * If the function is called with len == -1 then strlen is called to check
	 * the string length is less than INT32_MAX (2GB)
	 */
	if ((len < -1) || (len == -1 && strlen(str) > INT32_MAX)) {
		tok->err = json_tokener_error_size;
		return NULL;
	}

#ifdef HAVE_USELOCALE
	{
		locale_t duploc = duplocale(oldlocale);
		newloc = newlocale(LC_NUMERIC_MASK, "C", duploc);
		if (newloc == NULL) {
			freelocale(duploc);
			return NULL;
		}
		uselocale(newloc);
	}
#elif defined(HAVE_SETLOCALE)
	{
		char *tmplocale;
		tmplocale = setlocale(LC_NUMERIC, NULL);
		if (tmplocale)
			oldlocale = strdup(tmplocale);
		setlocale(LC_NUMERIC, "C");
	}
#endif

	while (PEEK_CHAR(c, tok)) // Note: c might be '\0' !
	{

	redo_char:
		switch (state) {

		case json_tokener_state_eatws:
			/* Advance until we change state */
			while (is_ws_char(c)) {
				if ((!ADVANCE_CHAR(str, tok)) || (!PEEK_CHAR(c, tok)))
					goto out;
			}
			if (c == '/' && !(tok->flags & JSON_TOKENER_STRICT)) {
				printbuf_reset(tok->pb);
				printbuf_memappend_fast(tok->pb, &c, 1);
				state = json_tokener_state_comment_start;
			} else {
				state = saved_state;
				goto redo_char;
			}
			break;

		case json_tokener_state_start:
			switch (c) {
			case '{':
				state = json_tokener_state_eatws;
				saved_state = json_tokener_state_object_field_start;
				current = json_object_new_object();
				if (current == NULL)
					goto out;
				break;
			case '[':
				state = json_tokener_state_eatws;
				saved_state = json_tokener_state_array;
				current = json_object_new_array();
				if (current == NULL)
					goto out;
				break;
			case 'I':
			case 'i':
				state = json_tokener_state_inf;
				printbuf_reset(tok->pb);
				tok->st_pos = 0;
				goto redo_char;
			case 'N':
			case 'n':
				state = json_tokener_state_null; // or NaN
				printbuf_reset(tok->pb);
				tok->st_pos = 0;
				goto redo_char;
			case '\'':
				if (tok->flags & JSON_TOKENER_STRICT) {
					/* in STRICT mode only double-quote are allowed */
					tok->err = json_tokener_error_parse_unexpected;
					goto out;
				}
				/* FALLTHRU */
			case '"':
				state = json_tokener_state_string;
				printbuf_reset(tok->pb);
				tok->quote_char = c;
				break;
			case 'T':
			case 't':
			case 'F':
			case 'f':
				state = json_tokener_state_boolean;
				printbuf_reset(tok->pb);
				tok->st_pos = 0;
				goto redo_char;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
			case '-':
				state = json_tokener_state_number;
				printbuf_reset(tok->pb);
				tok->is_double = 0;
				goto redo_char;
			default:
				tok->err = json_tokener_error_parse_unexpected;
				goto out;
			}
			break;

		case json_tokener_state_finish:
			if (tok->depth == 0)
				goto out;
			obj = json_object_get(current);
			json_tokener_reset_level(tok, tok->depth);
			tok->depth--;
			goto redo_char;

		case json_tokener_state_inf: /* aka starts with 'i' (or 'I', or "-i", or "-I") */
		{
			/* If we were guaranteed to have len set, then we could (usually) handle
			 * the entire "Infinity" check in a single strncmp (strncasecmp), but
			 * since len might be -1 (i.e. "read until \0"), we need to check it
			 * a character at a time.
			 * Trying to handle it both ways would make this code considerably more
			 * complicated with likely little performance benefit.
			 */
			int is_negative = 0;

			/* Note: tok->st_pos must be 0 when state is set to json_tokener_state_inf */
			while (tok->st_pos < (int)json_inf_str_len) {
				char inf_char = *str;
				if (inf_char != json_inf_str[tok->st_pos] &&
				    ((tok->flags & JSON_TOKENER_STRICT) || inf_char != json_inf_str_invert[tok->st_pos])) {
					tok->err = json_tokener_error_parse_unexpected;
					goto out;
				}
				tok->st_pos++;
				(void)ADVANCE_CHAR(str, tok);
				if (!PEEK_CHAR(c, tok)) {
					/* out of input chars, for now at least */
					goto out;
				}
			}
			/* We checked the full length of "Infinity", so create the object.
			 * When handling -Infinity, the number parsing code will have dropped
			 * the "-" into tok->pb for us, so check it now.
			 */
			if (printbuf_length(tok->pb) > 0 && *(tok->pb->buf) == '-') {
				is_negative = 1;
			}
			current = json_object_new_double(is_negative ? -INFINITY : INFINITY);
			if (current == NULL)
				goto out;
			saved_state = json_tokener_state_finish;
			state = json_tokener_state_eatws;
			goto redo_char;
		} break;
		case json_tokener_state_null: /* aka starts with 'n' */
		{
			int size;
			int size_nan;
			printbuf_memappend_fast(tok->pb, &c, 1);
			size = json_min(tok->st_pos + 1, json_null_str_len);
			size_nan = json_min(tok->st_pos + 1, json_nan_str_len);
			if ((!(tok->flags & JSON_TOKENER_STRICT) && strncasecmp(json_null_str, tok->pb->buf, size) == 0) ||
			    (strncmp(json_null_str, tok->pb->buf, size) == 0)) {
				if (tok->st_pos == json_null_str_len) {
					current = NULL;
					saved_state = json_tokener_state_finish;
					state = json_tokener_state_eatws;
					goto redo_char;
				}
			} else if ((!(tok->flags & JSON_TOKENER_STRICT) &&
			            strncasecmp(json_nan_str, tok->pb->buf, size_nan) == 0) ||
			           (strncmp(json_nan_str, tok->pb->buf, size_nan) == 0)) {
				if (tok->st_pos == json_nan_str_len) {
					current = json_object_new_double(NAN);
					if (current == NULL)
						goto out;
					saved_state = json_tokener_state_finish;
					state = json_tokener_state_eatws;
					goto redo_char;
				}
			} else {
				tok->err = json_tokener_error_parse_null;
				goto out;
			}
			tok->st_pos++;
		} break;

		case json_tokener_state_comment_start:
			if (c == '*') {
				state = json_tokener_state_comment;
			} else if (c == '/') {
				state = json_tokener_state_comment_eol;
			} else {
				tok->err = json_tokener_error_parse_comment;
				goto out;
			}
			printbuf_memappend_fast(tok->pb, &c, 1);
			break;

		case json_tokener_state_comment: {
			/* Advance until we change state */
			const char *case_start = str;
			while (c != '*') {
				if (!ADVANCE_CHAR(str, tok) || !PEEK_CHAR(c, tok)) {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					goto out;
				}
			}
			printbuf_memappend_fast(tok->pb, case_start, 1 + str - case_start);
			state = json_tokener_state_comment_end;
		} break;

		case json_tokener_state_comment_eol: {
			/* Advance until we change state */
			const char *case_start = str;
			while (c != '\n') {
				if (!ADVANCE_CHAR(str, tok) || !PEEK_CHAR(c, tok)) {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					goto out;
				}
			}
			printbuf_memappend_fast(tok->pb, case_start, str - case_start);
			state = json_tokener_state_eatws;
		} break;

		case json_tokener_state_comment_end:
			printbuf_memappend_fast(tok->pb, &c, 1);
			if (c == '/') {
				state = json_tokener_state_eatws;
			} else {
				state = json_tokener_state_comment;
			}
			break;

		case json_tokener_state_string: {
			/* Advance until we change state */
			const char *case_start = str;
			while (1) {
				if (c == tok->quote_char) {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					current = json_object_new_string_len(tok->pb->buf, tok->pb->bpos);
					if (current == NULL)
						goto out;
					saved_state = json_tokener_state_finish;
					state = json_tokener_state_eatws;
					break;
				} else if (c == '\\') {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					saved_state = json_tokener_state_string;
					state = json_tokener_state_string_escape;
					break;
				}
				if (!ADVANCE_CHAR(str, tok) || !PEEK_CHAR(c, tok)) {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					goto out;
				}
			}
		} break;

		case json_tokener_state_string_escape:
			switch (c) {
			case '"':
			case '\\':
			case '/':
				printbuf_memappend_fast(tok->pb, &c, 1);
				state = saved_state;
				break;
			case 'b':
			case 'n':
			case 'r':
			case 't':
			case 'f':
				if (c == 'b')
					printbuf_memappend_fast(tok->pb, "\b", 1);
				else if (c == 'n')
					printbuf_memappend_fast(tok->pb, "\n", 1);
				else if (c == 'r')
					printbuf_memappend_fast(tok->pb, "\r", 1);
				else if (c == 't')
					printbuf_memappend_fast(tok->pb, "\t", 1);
				else if (c == 'f')
					printbuf_memappend_fast(tok->pb, "\f", 1);
				state = saved_state;
				break;
			case 'u':
				tok->ucs_char = 0;
				tok->st_pos = 0;
				state = json_tokener_state_escape_unicode;
				break;
			default:
				tok->err = json_tokener_error_parse_string;
				goto out;
			}
			break;

			// ===================================================

		case json_tokener_state_escape_unicode: {
			/* Handle a 4-byte \uNNNN sequence, or two sequences if a surrogate pair */
			while (1) {
				if (!c || !is_hex_char(c)) {
					tok->err = json_tokener_error_parse_string;
					goto out;
				}
				tok->ucs_char |= ((unsigned int)jt_hexdigit(c) << ((3 - tok->st_pos) * 4));
				tok->st_pos++;
				if (tok->st_pos >= 4)
					break;

				(void)ADVANCE_CHAR(str, tok);
				if (!PEEK_CHAR(c, tok)) {
					/*
					 * We're out of characters in the current call to
					 * json_tokener_parse(), but a subsequent call might
					 * provide us with more, so leave our current state
					 * as-is (including tok->high_surrogate) and return.
					 */
					goto out;
				}
			}
			tok->st_pos = 0;

			/* Now, we have a full \uNNNN sequence in tok->ucs_char */

			/* If the *previous* sequence was a high surrogate ... */
			if (tok->high_surrogate) {
				if (IS_LOW_SURROGATE(tok->ucs_char)) {
					/* Recalculate the ucs_char, then fall thru to process normally */
					tok->ucs_char = DECODE_SURROGATE_PAIR(tok->high_surrogate, tok->ucs_char);
				} else {
					/* High surrogate was not followed by a low surrogate
					 * Replace the high and process the rest normally
					 */
					printbuf_memappend_fast(tok->pb, (char *)utf8_replacement_char, 3);
				}
				tok->high_surrogate = 0;
			}

			if (tok->ucs_char < 0x80) {
				unsigned char unescaped_utf[1];
				unescaped_utf[0] = tok->ucs_char;
				printbuf_memappend_fast(tok->pb, (char *)unescaped_utf, 1);
			} else if (tok->ucs_char < 0x800) {
				unsigned char unescaped_utf[2];
				unescaped_utf[0] = 0xc0 | (tok->ucs_char >> 6);
				unescaped_utf[1] = 0x80 | (tok->ucs_char & 0x3f);
				printbuf_memappend_fast(tok->pb, (char *)unescaped_utf, 2);
			} else if (IS_HIGH_SURROGATE(tok->ucs_char)) {
				/*
				 * The next two characters should be \u, HOWEVER,
				 * we can't simply peek ahead here, because the
				 * characters we need might not be passed to us
				 * until a subsequent call to json_tokener_parse.
				 * Instead, transition throug a couple of states.
				 * (now):
				 *   _escape_unicode => _unicode_need_escape
				 * (see a '\\' char):
				 *   _unicode_need_escape => _unicode_need_u
				 * (see a 'u' char):
				 *   _unicode_need_u => _escape_unicode
				 *      ...and we'll end up back around here.
				 */
				tok->high_surrogate = tok->ucs_char;
				tok->ucs_char = 0;
				state = json_tokener_state_escape_unicode_need_escape;
				break;
			} else if (IS_LOW_SURROGATE(tok->ucs_char)) {
				/* Got a low surrogate not preceded by a high */
				printbuf_memappend_fast(tok->pb, (char *)utf8_replacement_char, 3);
			} else if (tok->ucs_char < 0x10000) {
				unsigned char unescaped_utf[3];
				unescaped_utf[0] = 0xe0 | (tok->ucs_char >> 12);
				unescaped_utf[1] = 0x80 | ((tok->ucs_char >> 6) & 0x3f);
				unescaped_utf[2] = 0x80 | (tok->ucs_char & 0x3f);
				printbuf_memappend_fast(tok->pb, (char *)unescaped_utf, 3);
			} else if (tok->ucs_char < 0x110000) {
				unsigned char unescaped_utf[4];
				unescaped_utf[0] = 0xf0 | ((tok->ucs_char >> 18) & 0x07);
				unescaped_utf[1] = 0x80 | ((tok->ucs_char >> 12) & 0x3f);
				unescaped_utf[2] = 0x80 | ((tok->ucs_char >> 6) & 0x3f);
				unescaped_utf[3] = 0x80 | (tok->ucs_char & 0x3f);
				printbuf_memappend_fast(tok->pb, (char *)unescaped_utf, 4);
			} else {
				/* Don't know what we got--insert the replacement char */
				printbuf_memappend_fast(tok->pb, (char *)utf8_replacement_char, 3);
			}
			state = saved_state; // i.e. _state_string or _state_object_field
		} break;

		case json_tokener_state_escape_unicode_need_escape:
			// We get here after processing a high_surrogate
			// require a '\\' char
			if (!c || c != '\\') {
				/* Got a high surrogate without another sequence following
				 * it.  Put a replacement char in for the high surrogate
				 * and pop back up to _state_string or _state_object_field.
				 */
				printbuf_memappend_fast(tok->pb, (char *)utf8_replacement_char, 3);
				tok->high_surrogate = 0;
				tok->ucs_char = 0;
				tok->st_pos = 0;
				state = saved_state;
				goto redo_char;
			}
			state = json_tokener_state_escape_unicode_need_u;
			break;

		case json_tokener_state_escape_unicode_need_u:
			/* We already had a \ char, check that it's \u */
			if (!c || c != 'u') {
				/* Got a high surrogate with some non-unicode escape
				 * sequence following it.
				 * Put a replacement char in for the high surrogate
				 * and handle the escape sequence normally.
				 */
				printbuf_memappend_fast(tok->pb, (char *)utf8_replacement_char, 3);
				tok->high_surrogate = 0;
				tok->ucs_char = 0;
				tok->st_pos = 0;
				state = json_tokener_state_string_escape;
				goto redo_char;
			}
			state = json_tokener_state_escape_unicode;
			break;

			// ===================================================

		case json_tokener_state_boolean: {
			int size1, size2;
			printbuf_memappend_fast(tok->pb, &c, 1);
			size1 = json_min(tok->st_pos + 1, json_true_str_len);
			size2 = json_min(tok->st_pos + 1, json_false_str_len);
			if ((!(tok->flags & JSON_TOKENER_STRICT) && strncasecmp(json_true_str, tok->pb->buf, size1) == 0) ||
			    (strncmp(json_true_str, tok->pb->buf, size1) == 0)) {
				if (tok->st_pos == json_true_str_len) {
					current = json_object_new_boolean(1);
					if (current == NULL)
						goto out;
					saved_state = json_tokener_state_finish;
					state = json_tokener_state_eatws;
					goto redo_char;
				}
			} else if ((!(tok->flags & JSON_TOKENER_STRICT) && strncasecmp(json_false_str, tok->pb->buf, size2) == 0) ||
			           (strncmp(json_false_str, tok->pb->buf, size2) == 0)) {
				if (tok->st_pos == json_false_str_len) {
					current = json_object_new_boolean(0);
					if (current == NULL)
						goto out;
					saved_state = json_tokener_state_finish;
					state = json_tokener_state_eatws;
					goto redo_char;
				}
			} else {
				tok->err = json_tokener_error_parse_boolean;
				goto out;
			}
			tok->st_pos++;
		} break;

		case json_tokener_state_number: {
			/* Advance until we change state */
			const char *case_start = str;
			int case_len = 0;
			int is_exponent = 0;
			int neg_sign_ok = 1;
			int pos_sign_ok = 0;
			if (printbuf_length(tok->pb) > 0) {
				/* We don't save all state from the previous incremental parse
				   so we need to re-generate it based on the saved string so far.
				 */
				char *e_loc = strchr(tok->pb->buf, 'e');
				if (!e_loc)
					e_loc = strchr(tok->pb->buf, 'E');
				if (e_loc) {
					char *last_saved_char = &tok->pb->buf[printbuf_length(tok->pb) - 1];
					is_exponent = 1;
					pos_sign_ok = neg_sign_ok = 1;
					/* If the "e" isn't at the end, we can't start with a '-' */
					if (e_loc != last_saved_char) {
						neg_sign_ok = 0;
						pos_sign_ok = 0;
					}
					// else leave it set to 1, i.e. start of the new input
				}
			}

			while (c && ((c >= '0' && c <= '9') || (!is_exponent && (c == 'e' || c == 'E')) ||
			             (neg_sign_ok && c == '-') || (pos_sign_ok && c == '+') || (!tok->is_double && c == '.'))) {
				pos_sign_ok = neg_sign_ok = 0;
				++case_len;

				/* non-digit characters checks */
				/* note: since the main loop condition to get here was
				 * an input starting with 0-9 or '-', we are
				 * protected from input starting with '.' or
				 * e/E.
				 */
				switch (c) {
				case '.':
					tok->is_double = 1;
					pos_sign_ok = 1;
					neg_sign_ok = 1;
					break;
				case 'e': /* FALLTHRU */
				case 'E':
					is_exponent = 1;
					tok->is_double = 1;
					/* the exponent part can begin with a negative sign */
					pos_sign_ok = neg_sign_ok = 1;
					break;
				default:
					break;
				}

				if (!ADVANCE_CHAR(str, tok) || !PEEK_CHAR(c, tok)) {
					printbuf_memappend_fast(tok->pb, case_start, case_len);
					goto out;
				}
			}
			/*
			    Now we know c isn't a valid number char, but check whether
			    it might have been intended to be, and return a potentially
			    more understandable error right away.
			    However, if we're at the top-level, use the number as-is
			    because c can be part of a new object to parse on the
			    next call to json_tokener_parse().
			 */
			if (tok->depth > 0 && c != ',' && c != ']' && c != '}' && c != '/' && c != 'I' && c != 'i' &&
			    !is_ws_char(c)) {
				tok->err = json_tokener_error_parse_number;
				goto out;
			}
			if (case_len > 0)
				printbuf_memappend_fast(tok->pb, case_start, case_len);

			// Check for -Infinity
			if (tok->pb->buf[0] == '-' && case_len <= 1 && (c == 'i' || c == 'I')) {
				state = json_tokener_state_inf;
				tok->st_pos = 0;
				goto redo_char;
			}
			if (tok->is_double && !(tok->flags & JSON_TOKENER_STRICT)) {
				/* Trim some chars off the end, to allow things
				   like "123e+" to parse ok. */
				while (printbuf_length(tok->pb) > 1) {
					char last_char = tok->pb->buf[printbuf_length(tok->pb) - 1];
					if (last_char != 'e' && last_char != 'E' && last_char != '-' && last_char != '+') {
						break;
					}
					tok->pb->buf[printbuf_length(tok->pb) - 1] = '\0';
					printbuf_length(tok->pb)--;
				}
			}
		}
			{
				int64_t num64;
				uint64_t numuint64;
				double numd;
				if (!tok->is_double && tok->pb->buf[0] == '-' && json_parse_int64(tok->pb->buf, &num64) == 0) {
					current = json_object_new_int64(num64);
					if (current == NULL)
						goto out;
				} else if (!tok->is_double && tok->pb->buf[0] != '-' &&
				           json_parse_uint64(tok->pb->buf, &numuint64) == 0) {
					if (numuint64 && tok->pb->buf[0] == '0' && (tok->flags & JSON_TOKENER_STRICT)) {
						tok->err = json_tokener_error_parse_number;
						goto out;
					}
					if (numuint64 <= INT64_MAX) {
						num64 = (uint64_t)numuint64;
						current = json_object_new_int64(num64);
						if (current == NULL)
							goto out;
					} else {
						current = json_object_new_uint64(numuint64);
						if (current == NULL)
							goto out;
					}
				} else if (tok->is_double &&
				           json_tokener_parse_double(tok->pb->buf, printbuf_length(tok->pb), &numd) == 0) {
					current = json_object_new_double_s(numd, tok->pb->buf);
					if (current == NULL)
						goto out;
				} else {
					tok->err = json_tokener_error_parse_number;
					goto out;
				}
				saved_state = json_tokener_state_finish;
				state = json_tokener_state_eatws;
				goto redo_char;
			}
			break;

		case json_tokener_state_array_after_sep:
		case json_tokener_state_array:
			if (c == ']') {
				// Minimize memory usage; assume parsed objs are unlikely to be changed
				json_object_array_shrink(current, 0);

				if (state == json_tokener_state_array_after_sep && (tok->flags & JSON_TOKENER_STRICT)) {
					tok->err = json_tokener_error_parse_unexpected;
					goto out;
				}
				saved_state = json_tokener_state_finish;
				state = json_tokener_state_eatws;
			} else {
				if (tok->depth >= tok->max_depth - 1) {
					tok->err = json_tokener_error_depth;
					goto out;
				}
				state = json_tokener_state_array_add;
				tok->depth++;
				json_tokener_reset_level(tok, tok->depth);
				goto redo_char;
			}
			break;

		case json_tokener_state_array_add:
			if (json_object_array_add(current, obj) != 0)
				goto out;
			saved_state = json_tokener_state_array_sep;
			state = json_tokener_state_eatws;
			goto redo_char;

		case json_tokener_state_array_sep:
			if (c == ']') {
				// Minimize memory usage; assume parsed objs are unlikely to be changed
				json_object_array_shrink(current, 0);

				saved_state = json_tokener_state_finish;
				state = json_tokener_state_eatws;
			} else if (c == ',') {
				saved_state = json_tokener_state_array_after_sep;
				state = json_tokener_state_eatws;
			} else {
				tok->err = json_tokener_error_parse_array;
				goto out;
			}
			break;

		case json_tokener_state_object_field_start:
		case json_tokener_state_object_field_start_after_sep:
			if (c == '}') {
				if (state == json_tokener_state_object_field_start_after_sep && (tok->flags & JSON_TOKENER_STRICT)) {
					tok->err = json_tokener_error_parse_unexpected;
					goto out;
				}
				saved_state = json_tokener_state_finish;
				state = json_tokener_state_eatws;
			} else if (c == '"' || c == '\'') {
				tok->quote_char = c;
				printbuf_reset(tok->pb);
				state = json_tokener_state_object_field;
			} else {
				tok->err = json_tokener_error_parse_object_key_name;
				goto out;
			}
			break;

		case json_tokener_state_object_field: {
			/* Advance until we change state */
			const char *case_start = str;
			while (1) {
				if (c == tok->quote_char) {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					obj_field_name = strdup(tok->pb->buf);
					saved_state = json_tokener_state_object_field_end;
					state = json_tokener_state_eatws;
					break;
				} else if (c == '\\') {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					saved_state = json_tokener_state_object_field;
					state = json_tokener_state_string_escape;
					break;
				}
				if (!ADVANCE_CHAR(str, tok) || !PEEK_CHAR(c, tok)) {
					printbuf_memappend_fast(tok->pb, case_start, str - case_start);
					goto out;
				}
			}
		} break;

		case json_tokener_state_object_field_end:
			if (c == ':') {
				saved_state = json_tokener_state_object_value;
				state = json_tokener_state_eatws;
			} else {
				tok->err = json_tokener_error_parse_object_key_sep;
				goto out;
			}
			break;

		case json_tokener_state_object_value:
			if (tok->depth >= tok->max_depth - 1) {
				tok->err = json_tokener_error_depth;
				goto out;
			}
			state = json_tokener_state_object_value_add;
			tok->depth++;
			json_tokener_reset_level(tok, tok->depth);
			goto redo_char;

		case json_tokener_state_object_value_add:
			json_object_object_add(current, obj_field_name, obj);
			free(obj_field_name);
			obj_field_name = NULL;
			saved_state = json_tokener_state_object_sep;
			state = json_tokener_state_eatws;
			goto redo_char;

		case json_tokener_state_object_sep:
			/* { */
			if (c == '}') {
				saved_state = json_tokener_state_finish;
				state = json_tokener_state_eatws;
			} else if (c == ',') {
				saved_state = json_tokener_state_object_field_start_after_sep;
				state = json_tokener_state_eatws;
			} else {
				tok->err = json_tokener_error_parse_object_value_sep;
				goto out;
			}
			break;
		}
		(void)ADVANCE_CHAR(str, tok);
		if (!c) // This is the char *before* advancing
			break;
	} /* while(PEEK_CHAR) */

out:
	if ((tok->flags & JSON_TOKENER_VALIDATE_UTF8) && (nBytes != 0)) {
		tok->err = json_tokener_error_parse_utf8_string;
	}
	if (c && (state == json_tokener_state_finish) && (tok->depth == 0) &&
	    (tok->flags & (JSON_TOKENER_STRICT | JSON_TOKENER_ALLOW_TRAILING_CHARS)) == JSON_TOKENER_STRICT) {
		/* unexpected char after JSON data */
		tok->err = json_tokener_error_parse_unexpected;
	}
	if (!c) {
		/* We hit an eof char (0) */
		if (state != json_tokener_state_finish && saved_state != json_tokener_state_finish)
			tok->err = json_tokener_error_parse_eof;
	}

#ifdef HAVE_USELOCALE
	uselocale(oldlocale);
	freelocale(newloc);
#elif defined(HAVE_SETLOCALE)
	setlocale(LC_NUMERIC, oldlocale);
	free(oldlocale);
#endif

	if (tok->err == json_tokener_success) {
		json_object *ret = json_object_get(current);
		int ii;

		/* Partially reset, so we parse additional objects on subsequent calls. */
		for (ii = tok->depth; ii >= 0; ii--)
			json_tokener_reset_level(tok, ii);
		return ret;
	}

	return NULL;
}

const char *json_tokener_error_desc(enum json_tokener_error jerr) {
	int jerr_int = (int)jerr;
	if (jerr_int < 0 || jerr_int >= (int)(sizeof(json_tokener_errors) / sizeof(json_tokener_errors[0])))
		return "Unknown error, "
		       "invalid json_tokener_error value passed to json_tokener_error_desc()";
	return json_tokener_errors[jerr];
}

void json_tokener_free(struct json_tokener *tok) {
	json_tokener_reset(tok);
	if (tok->pb)
		printbuf_free(tok->pb);
	free(tok->stack);
	free(tok);
}

} // namespace json
