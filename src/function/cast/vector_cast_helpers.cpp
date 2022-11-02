#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

static bool IsNull(const char *buf, idx_t start_pos, Vector &child, idx_t row_idx) {
	if (buf[start_pos] == 'N' && buf[start_pos + 1] == 'U' && buf[start_pos + 2] == 'L' && buf[start_pos + 3] == 'L') {
		if (child.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			ConstantVector::SetNull(child, true);
		} else {
			FlatVector::SetNull(child, row_idx, true);
		}
		return true;
	}
	return false;
}

inline static void SkipWhitespace(const char *buf, idx_t &pos, idx_t len) {
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}
}

static idx_t StringTrim(const char *buf, idx_t &start_pos, idx_t pos) {
	idx_t trailing_whitespace = 0;
	while (StringUtil::CharacterIsSpace(buf[pos - trailing_whitespace - 1])) {
		trailing_whitespace++;
	}
	if ((buf[start_pos] == '"' && buf[pos - trailing_whitespace - 1] == '"') ||
	    (buf[start_pos] == '\'' && buf[pos - trailing_whitespace - 1] == '\'')) {
		start_pos++;
		trailing_whitespace++;
	}
	return (pos - trailing_whitespace);
}

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
		if ((pos - start_pos) == 4 && IsNull(buf, start_pos, child, child_start)) {
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

static bool SkipToClose(idx_t &idx, const char *buf, idx_t &len, idx_t &lvl, char close_bracket) {
	idx++;

	while (idx < len) {
		if (buf[idx] == '"' || buf[idx] == '\'') {
			if (!SkipToCloseQuotes(idx, buf, len)) {
				return false;
			}
		} else if (buf[idx] == '{') {
			if (!SkipToClose(idx, buf, len, lvl, '}')) {
				return false;
			}
		} else if (buf[idx] == '[') {
			if (!SkipToClose(idx, buf, len, lvl, ']')) {
				return false;
			}
			lvl++;
		} else if (buf[idx] == close_bracket) {
			if (close_bracket == ']') {
				lvl--;
			}
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

	SkipWhitespace(buf, pos, len);
	if (pos == len || buf[pos] != '[') {
		return false;
	}

	SkipWhitespace(buf, ++pos, len);
	idx_t start_pos = pos;
	while (pos < len) {
		if (buf[pos] == '[') {
			if (!SkipToClose(pos, buf, len, ++lvl, ']')) {
				return false;
			}
		} else if (buf[pos] == '"' || buf[pos] == '\'') {
			SkipToCloseQuotes(pos, buf, len);
		} else if (buf[pos] == '{') {
			idx_t struct_lvl = 0;
			SkipToClose(pos, buf, len, struct_lvl, '}');
		} else if (buf[pos] == ',' || buf[pos] == ']') {
			idx_t trailing_whitespace = 0;
			while (StringUtil::CharacterIsSpace(buf[pos - trailing_whitespace - 1])) {
				trailing_whitespace++;
			}
			if (!(buf[pos] == ']' && start_pos == pos)) {
				state.HandleValue(buf, start_pos, pos - trailing_whitespace);
			} // else the list is empty
			if (buf[pos] == ']') {
				lvl--;
				break;
			}
			SkipWhitespace(buf, ++pos, len);
			start_pos = pos;
			continue;
		}
		pos++;
	}
	SkipWhitespace(buf, ++pos, len);
	return (pos == len && lvl == 0);
}

bool VectorStringToList::SplitStringifiedList(const string_t &input, string_t *child_data, idx_t &child_start,
                                              Vector &child) {
	SplitStringOperation state(child_data, child_start, child);
	return SplitStringifiedListInternal<SplitStringOperation>(input, state);
}

idx_t VectorStringToList::CountParts(const string_t &input) {
	CountPartOperation state;
	SplitStringifiedListInternal<CountPartOperation>(input, state);
	return state.count;
}

static bool FindKey(const char *buf, idx_t len, idx_t &pos) {
	while (pos < len) {
		if (buf[pos] == ':') {
			return true;
		}
		pos++;
	}
	return false;
}

static bool FindValue(const char *buf, idx_t len, idx_t &pos, Vector &varchar_child, idx_t &row_idx,
                      ValidityMask *child_mask) {
	auto start_pos = pos;
	idx_t lvl = 0;
	while (pos < len) {
		if (buf[pos] == '"' || buf[pos] == '\'') {
			SkipToCloseQuotes(pos, buf, len);
		} else if (buf[pos] == '{') {
			SkipToClose(pos, buf, len, lvl, '}');
		} else if (buf[pos] == '[') {
			SkipToClose(pos, buf, len, lvl, ']');
		} else if (buf[pos] == ',' || buf[pos] == '}') {
			idx_t end_pos = StringTrim(buf, start_pos, pos);
			if ((end_pos - start_pos) == 4 && IsNull(buf, start_pos, varchar_child, row_idx)) {
				return true;
			}
			FlatVector::GetData<string_t>(varchar_child)[row_idx] =
			    StringVector::AddString(varchar_child, buf + start_pos, end_pos - start_pos);
			child_mask->SetValid(row_idx); // any child not set to valid will remain invalid
			return true;
		}
		pos++;
	}
	return false;
}

bool VectorStringToStruct::SplitStruct(string_t &input, std::vector<std::unique_ptr<Vector>> &varchar_vectors,
                                       idx_t &row_idx, string_map_t<idx_t> &child_names,
                                       std::vector<ValidityMask *> &child_masks) {
	const char *buf = input.GetDataUnsafe();
	idx_t len = input.GetSize();
	idx_t pos = 0;
	idx_t child_idx;

	SkipWhitespace(buf, pos, len);
	if (pos == len || buf[pos] != '{') {
		return false;
	}
	SkipWhitespace(buf, ++pos, len);
	if (buf[pos] == '}') {
		pos++;
	} else {
		while (pos < len) {
			auto key_start = pos;
			if (!FindKey(buf, len, pos)) {
				return false;
			}
			auto key_end = StringTrim(buf, key_start, pos);
			string_t found_key(buf + key_start, key_end - key_start);

			auto it = child_names.find(found_key);
			if (it == child_names.end()) {
				return false; // false key
			}
			child_idx = it->second;
			SkipWhitespace(buf, ++pos, len);
			if (!FindValue(buf, len, pos, *varchar_vectors[child_idx], row_idx, child_masks[child_idx])) {
				return false;
			}
			SkipWhitespace(buf, ++pos, len);
		}
	}
	SkipWhitespace(buf, pos, len);
	return (pos == len);
}

} // namespace duckdb
