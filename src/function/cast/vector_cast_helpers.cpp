#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

struct CountPartOperation {
	idx_t count = 0;

	void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {
		count++;
	}
};

struct SplitStringOperation {
	SplitStringOperation(string_t *child_data, idx_t &child_start, Vector &child)
	    : child_data(child_data), child_start(child_start), child(child) {
	}

	string_t *child_data;
	idx_t &child_start;
	Vector &child;

	void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {

		if ((pos - start_pos) >= 4 && buf[start_pos] == 'N' && buf[start_pos + 1] == 'U' && buf[start_pos + 2] == 'L' &&
		    buf[start_pos + 3] == 'L') {
			FlatVector::SetNull(child, child_start, true);
			child_start++;
			return;
		}
		child_data[child_start] = StringVector::AddString(child, buf + start_pos, pos - start_pos);
		child_start++;
	}
};

static bool SkipToCloseQuotes(idx_t &pos, const char *buf, idx_t &len) {
	char quote = buf[pos];
	pos++;

	while (pos < len) {
		if (buf[pos] == quote) {
			return true;
		}
		pos++;
	}
	return false;
}

static bool SkipToClose(idx_t &idx, const char *buf, idx_t &len, idx_t &lvl) {
	while (idx < len) {
		if (buf[idx] == '[') {
			if (!SkipToClose(++idx, buf, len, lvl)) {
				return false;
			}
			lvl++;
			idx++;
		}
		if (buf[idx] == '"' || buf[idx] == '\'') {
			SkipToCloseQuotes(idx, buf, len);
		}
		if (buf[idx] == ']') {
			lvl--;
			return true;
		}
		idx++;
	}
	return false;
}

template <class OP>
static bool SplitStringifiedListInternal(const string_t &input, OP &state) {
	const char *buf = input.GetDataUnsafe();
	idx_t len = input.GetSize();
	idx_t lvl = 1;
	idx_t pos = 0;

	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}
	if (pos == len || buf[pos] != '[') {
		return false;
	}
	pos++;
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	idx_t start_pos = pos;
	while (pos < len) {
		if (buf[pos] == '[') {
			if (!SkipToClose(++pos, buf, len, ++lvl)) {
				return false;
			}
		} else if (buf[pos] == '"' || buf[pos] == '\'') {
			SkipToCloseQuotes(pos, buf, len);
		} else if (buf[pos] == ',' || buf[pos] == ']') {
			idx_t trailing_whitespace = 0;
			while (StringUtil::CharacterIsSpace(buf[pos - trailing_whitespace - 1])) {
				trailing_whitespace++;
			}
			if (!(buf[pos] == ']' && start_pos == (pos))) {
				state.HandleValue(buf, start_pos, pos - trailing_whitespace);
			} // else the list is empty
			if (buf[pos] == ']') {
				lvl--;
				break;
			}
			while (pos + 1 < len && StringUtil::CharacterIsSpace(buf[pos + 1])) {
				pos++;
			}
			start_pos = pos + 1;
		}
		pos++;
	}
	pos++;
	while (pos < len) {
		if (!StringUtil::CharacterIsSpace(buf[pos])) {
			return false;
		}
		pos++;
	}
	if (lvl != 0) {
		return false;
	}
	return true;
}

bool VectorStringifiedListParser::SplitStringifiedList(const string_t &input, string_t *child_data, idx_t &child_start,
                                                       Vector &child) {
	SplitStringOperation state(child_data, child_start, child);
	return SplitStringifiedListInternal<SplitStringOperation>(input, state);
}

idx_t VectorStringifiedListParser::CountParts(const string_t &input) {
	CountPartOperation state;
	SplitStringifiedListInternal<CountPartOperation>(input, state);
	return state.count;
}
} // namespace duckdb
