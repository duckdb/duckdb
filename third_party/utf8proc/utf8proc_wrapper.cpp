#include "utf8proc_wrapper.hpp"
#include "utf8proc.hpp"

using namespace std;

namespace duckdb {

// This function efficiently checks if a string is valid UTF8.
// It was originally written by Sjoerd Mullender.

// Here is the table that makes it work:

// B 		= Number of Bytes in UTF8 encoding
// C_MIN 	= First Unicode code point
// C_MAX 	= Last Unicode code point
// B1 		= First Byte Prefix

// 	B	C_MIN		C_MAX		B1
//	1	U+000000	U+00007F		0xxxxxxx
//	2	U+000080	U+0007FF		110xxxxx
//	3	U+000800	U+00FFFF		1110xxxx
//	4	U+010000	U+10FFFF		11110xxx

static void AssignInvalidUTF8Reason(UnicodeInvalidReason *invalid_reason, size_t *invalid_pos, size_t pos, UnicodeInvalidReason reason) {
	if (invalid_reason) {
		*invalid_reason = reason;
	}
	if (invalid_pos) {
		*invalid_pos = pos;
	}
}

template <const int nextra_bytes, const int mask>
static inline UnicodeType
UTF8ExtraByteLoop(const int first_pos_seq, int utf8char, size_t& i,
				  const char *s, const size_t len, UnicodeInvalidReason *invalid_reason, size_t *invalid_pos) {
	if ((len - i) < (nextra_bytes + 1)) {
		/* incomplete byte sequence */
		AssignInvalidUTF8Reason(invalid_reason, invalid_pos, first_pos_seq, UnicodeInvalidReason::BYTE_MISMATCH);
		return UnicodeType::INVALID;
	}
	for (size_t j = 0 ; j < nextra_bytes; j++) {
		int c = (int) s[++i];
		/* now validate the extra bytes */
		if ((c & 0xC0) != 0x80) {
			/* extra byte is not in the format 10xxxxxx */
			AssignInvalidUTF8Reason(invalid_reason, invalid_pos, i, UnicodeInvalidReason::BYTE_MISMATCH);
			return UnicodeType::INVALID;
		}
		utf8char = (utf8char << 6) | (c & 0x3F);
	}
	if ((utf8char & mask) == 0) {
		/* invalid UTF-8 codepoint, not shortest possible */
		AssignInvalidUTF8Reason(invalid_reason, invalid_pos, first_pos_seq, UnicodeInvalidReason::INVALID_UNICODE);
		return UnicodeType::INVALID;
	}
	if (utf8char > 0x10FFFF) {
		/* value not representable by Unicode */
		AssignInvalidUTF8Reason(invalid_reason, invalid_pos, first_pos_seq, UnicodeInvalidReason::INVALID_UNICODE);
		return UnicodeType::INVALID;
	}
	if ((utf8char & 0x1FFF800) == 0xD800) {
		/* Unicode characters from U+D800 to U+DFFF are surrogate characters used by UTF-16 which are invalid in UTF-8 */
		AssignInvalidUTF8Reason(invalid_reason, invalid_pos, first_pos_seq, UnicodeInvalidReason::INVALID_UNICODE);
		return UnicodeType::INVALID;
	}
	return UnicodeType::UNICODE;
}

UnicodeType Utf8Proc::Analyze(const char *s, size_t len, UnicodeInvalidReason *invalid_reason, size_t *invalid_pos) {
	UnicodeType type = UnicodeType::ASCII;

	for (size_t i = 0; i < len; i++) {
		int c = (int) s[i];

		if ((c & 0x80) == 0) {
			continue;
		}
		int first_pos_seq = i;

		if ((c & 0xE0) == 0xC0) {
			/* 2 byte sequence */
			int utf8char = c & 0x1F;
			type = UTF8ExtraByteLoop<1, 0x000780>(first_pos_seq, utf8char, i, s, len, invalid_reason, invalid_pos);
		} else if ((c & 0xF0) == 0xE0) {
			/* 3 byte sequence */
			int utf8char = c & 0x0F;
			type = UTF8ExtraByteLoop<2, 0x00F800>(first_pos_seq, utf8char, i, s, len, invalid_reason, invalid_pos);
		} else if ((c & 0xF8) == 0xF0) {
			/* 4 byte sequence */
			int utf8char = c & 0x07;
			type = UTF8ExtraByteLoop<3, 0x1F0000>(first_pos_seq, utf8char, i, s, len, invalid_reason, invalid_pos);
		} else {
			/* invalid UTF-8 start byte */
			AssignInvalidUTF8Reason(invalid_reason, invalid_pos, i, UnicodeInvalidReason::BYTE_MISMATCH);
			return UnicodeType::INVALID;
		}
		if (type == UnicodeType::INVALID) {
			return type;
		}
	}
	return type;
}

char* Utf8Proc::Normalize(const char *s, size_t len) {
	assert(s);
	assert(Utf8Proc::Analyze(s, len) != UnicodeType::INVALID);
	return (char*) utf8proc_NFC((const utf8proc_uint8_t*) s, len);
}

bool Utf8Proc::IsValid(const char *s, size_t len) {
	return Utf8Proc::Analyze(s, len) != UnicodeType::INVALID;
}

size_t Utf8Proc::NextGraphemeCluster(const char *s, size_t len, size_t cpos) {
	return utf8proc_next_grapheme(s, len, cpos);
}

size_t Utf8Proc::PreviousGraphemeCluster(const char *s, size_t len, size_t cpos) {
	if (!Utf8Proc::IsValid(s, len)) {
		return cpos - 1;
	}
	size_t current_pos = 0;
	while(true) {
		size_t new_pos = NextGraphemeCluster(s, len, current_pos);
		if (new_pos <= current_pos || new_pos >= cpos) {
			return current_pos;
		}
		current_pos = new_pos;
	}
}

bool Utf8Proc::CodepointToUtf8(int cp, int &sz, char *c) {
	return utf8proc_codepoint_to_utf8(cp, sz, c);
}

int Utf8Proc::CodepointLength(int cp) {
	return utf8proc_codepoint_length(cp);
}

int32_t Utf8Proc::UTF8ToCodepoint(const char *c, int &sz) {
	return utf8proc_codepoint(c, sz);
}

size_t Utf8Proc::RenderWidth(const char *s, size_t len, size_t pos) {
    int sz;
    auto codepoint = duckdb::utf8proc_codepoint(s + pos, sz);
    auto properties = duckdb::utf8proc_get_property(codepoint);
    return properties->charwidth;
}

size_t Utf8Proc::RenderWidth(const std::string &str) {
	size_t render_width = 0;
	size_t pos = 0;
	while(pos < str.size()) {
		int sz;
		auto codepoint = duckdb::utf8proc_codepoint(str.c_str() + pos, sz);
		auto properties = duckdb::utf8proc_get_property(codepoint);
		render_width += properties->charwidth;
		pos += sz;
	}
	return render_width;
}

}
