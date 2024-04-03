#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

// ------- Helper functions for splitting string nested types  -------
static bool IsNull(const char *buf, idx_t start_pos, Vector &child, idx_t row_idx) {
	if (buf[start_pos] == 'N' && buf[start_pos + 1] == 'U' && buf[start_pos + 2] == 'L' && buf[start_pos + 3] == 'L') {
		FlatVector::SetNull(child, row_idx, true);
		return true;
	}
	return false;
}

inline static void SkipWhitespace(const char *buf, idx_t &pos, idx_t len) {
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}
}

static bool SkipToCloseQuotes(idx_t &pos, const char *buf, idx_t &len) {
	char quote = buf[pos];
	pos++;
	bool escaped = false;

	while (pos < len) {
		if (buf[pos] == '\\') {
			escaped = !escaped;
		} else {
			if (buf[pos] == quote && !escaped) {
				return true;
			}
			escaped = false;
		}
		pos++;
	}
	return false;
}

static bool SkipToClose(idx_t &idx, const char *buf, idx_t &len, idx_t &lvl, char close_bracket) {
	idx++;

	vector<char> brackets;
	brackets.push_back(close_bracket);
	while (idx < len) {
		if (buf[idx] == '"' || buf[idx] == '\'') {
			if (!SkipToCloseQuotes(idx, buf, len)) {
				return false;
			}
		} else if (buf[idx] == '{') {
			brackets.push_back('}');
		} else if (buf[idx] == '[') {
			brackets.push_back(']');
			lvl++;
		} else if (buf[idx] == brackets.back()) {
			if (buf[idx] == ']') {
				lvl--;
			}
			brackets.pop_back();
			if (brackets.empty()) {
				return true;
			}
		}
		idx++;
	}
	return false;
}

static idx_t StringTrim(const char *buf, idx_t &start_pos, idx_t pos) {
	idx_t trailing_whitespace = 0;
	while (pos > start_pos && StringUtil::CharacterIsSpace(buf[pos - trailing_whitespace - 1])) {
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

	bool HandleKey(const char *buf, idx_t start_pos, idx_t pos) {
		count++;
		return true;
	}
	void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {
		count++;
	}
};

// ------- LIST SPLIT -------
struct SplitStringListOperation {
	SplitStringListOperation(string_t *child_data, idx_t &child_start, Vector &child)
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
		if (start_pos > pos) {
			pos = start_pos;
		}
		child_data[child_start] = StringVector::AddString(child, buf + start_pos, pos - start_pos);
		child_start++;
	}
};

template <class OP>
static bool SplitStringListInternal(const string_t &input, OP &state) {
	const char *buf = input.GetData();
	idx_t len = input.GetSize();
	idx_t lvl = 1;
	idx_t pos = 0;
	bool seen_value = false;

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
		} else if ((buf[pos] == '"' || buf[pos] == '\'') && pos == start_pos) {
			SkipToCloseQuotes(pos, buf, len);
		} else if (buf[pos] == '{') {
			idx_t struct_lvl = 0;
			SkipToClose(pos, buf, len, struct_lvl, '}');
		} else if (buf[pos] == ',' || buf[pos] == ']') {
			idx_t trailing_whitespace = 0;
			while (StringUtil::CharacterIsSpace(buf[pos - trailing_whitespace - 1])) {
				trailing_whitespace++;
			}
			if (buf[pos] != ']' || start_pos != pos || seen_value) {
				state.HandleValue(buf, start_pos, pos - trailing_whitespace);
				seen_value = true;
			}
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

bool VectorStringToList::SplitStringList(const string_t &input, string_t *child_data, idx_t &child_start,
                                         Vector &child) {
	SplitStringListOperation state(child_data, child_start, child);
	return SplitStringListInternal<SplitStringListOperation>(input, state);
}

idx_t VectorStringToList::CountPartsList(const string_t &input) {
	CountPartOperation state;
	SplitStringListInternal<CountPartOperation>(input, state);
	return state.count;
}

// ------- MAP SPLIT -------
struct SplitStringMapOperation {
	SplitStringMapOperation(string_t *child_key_data, string_t *child_val_data, idx_t &child_start, Vector &varchar_key,
	                        Vector &varchar_val)
	    : child_key_data(child_key_data), child_val_data(child_val_data), child_start(child_start),
	      varchar_key(varchar_key), varchar_val(varchar_val) {
	}

	string_t *child_key_data;
	string_t *child_val_data;
	idx_t &child_start;
	Vector &varchar_key;
	Vector &varchar_val;

	bool HandleKey(const char *buf, idx_t start_pos, idx_t pos) {
		if ((pos - start_pos) == 4 && IsNull(buf, start_pos, varchar_key, child_start)) {
			FlatVector::SetNull(varchar_val, child_start, true);
			child_start++;
			return false;
		}
		child_key_data[child_start] = StringVector::AddString(varchar_key, buf + start_pos, pos - start_pos);
		return true;
	}

	void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {
		if ((pos - start_pos) == 4 && IsNull(buf, start_pos, varchar_val, child_start)) {
			child_start++;
			return;
		}
		child_val_data[child_start] = StringVector::AddString(varchar_val, buf + start_pos, pos - start_pos);
		child_start++;
	}
};

template <class OP>
static bool FindKeyOrValueMap(const char *buf, idx_t len, idx_t &pos, OP &state, bool key) {
	auto start_pos = pos;
	idx_t lvl = 0;
	while (pos < len) {
		if (buf[pos] == '"' || buf[pos] == '\'') {
			SkipToCloseQuotes(pos, buf, len);
		} else if (buf[pos] == '{') {
			SkipToClose(pos, buf, len, lvl, '}');
		} else if (buf[pos] == '[') {
			SkipToClose(pos, buf, len, lvl, ']');
		} else if (key && buf[pos] == '=') {
			idx_t end_pos = StringTrim(buf, start_pos, pos);
			return state.HandleKey(buf, start_pos, end_pos); // put string in KEY_child_vector
		} else if (!key && (buf[pos] == ',' || buf[pos] == '}')) {
			idx_t end_pos = StringTrim(buf, start_pos, pos);
			state.HandleValue(buf, start_pos, end_pos); // put string in VALUE_child_vector
			return true;
		}
		pos++;
	}
	return false;
}

template <class OP>
static bool SplitStringMapInternal(const string_t &input, OP &state) {
	const char *buf = input.GetData();
	idx_t len = input.GetSize();
	idx_t pos = 0;

	SkipWhitespace(buf, pos, len);
	if (pos == len || buf[pos] != '{') {
		return false;
	}
	SkipWhitespace(buf, ++pos, len);
	if (pos == len) {
		return false;
	}
	if (buf[pos] == '}') {
		SkipWhitespace(buf, ++pos, len);
		return (pos == len);
	}
	while (pos < len) {
		if (!FindKeyOrValueMap(buf, len, pos, state, true)) {
			return false;
		}
		SkipWhitespace(buf, ++pos, len);
		if (!FindKeyOrValueMap(buf, len, pos, state, false)) {
			return false;
		}
		SkipWhitespace(buf, ++pos, len);
	}
	return true;
}

bool VectorStringToMap::SplitStringMap(const string_t &input, string_t *child_key_data, string_t *child_val_data,
                                       idx_t &child_start, Vector &varchar_key, Vector &varchar_val) {
	SplitStringMapOperation state(child_key_data, child_val_data, child_start, varchar_key, varchar_val);
	return SplitStringMapInternal<SplitStringMapOperation>(input, state);
}

idx_t VectorStringToMap::CountPartsMap(const string_t &input) {
	CountPartOperation state;
	SplitStringMapInternal<CountPartOperation>(input, state);
	return state.count;
}

// ------- STRUCT SPLIT -------
static bool FindKeyStruct(const char *buf, idx_t len, idx_t &pos) {
	while (pos < len) {
		if (buf[pos] == ':') {
			return true;
		}
		pos++;
	}
	return false;
}

static bool FindValueStruct(const char *buf, idx_t len, idx_t &pos, Vector &varchar_child, idx_t &row_idx,
                            ValidityMask &child_mask) {
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
			child_mask.SetValid(row_idx); // any child not set to valid will remain invalid
			return true;
		}
		pos++;
	}
	return false;
}

bool VectorStringToStruct::SplitStruct(const string_t &input, vector<unique_ptr<Vector>> &varchar_vectors,
                                       idx_t &row_idx, string_map_t<idx_t> &child_names,
                                       vector<reference<ValidityMask>> &child_masks) {
	const char *buf = input.GetData();
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
			if (!FindKeyStruct(buf, len, pos)) {
				return false;
			}
			auto key_end = StringTrim(buf, key_start, pos);
			if (key_start >= key_end) {
				// empty key name unsupported
				return false;
			}
			string_t found_key(buf + key_start, UnsafeNumericCast<uint32_t>(key_end - key_start));

			auto it = child_names.find(found_key);
			if (it == child_names.end()) {
				return false; // false key
			}
			child_idx = it->second;
			SkipWhitespace(buf, ++pos, len);
			if (!FindValueStruct(buf, len, pos, *varchar_vectors[child_idx], row_idx, child_masks[child_idx].get())) {
				return false;
			}
			SkipWhitespace(buf, ++pos, len);
		}
	}
	SkipWhitespace(buf, pos, len);
	return (pos == len);
}

} // namespace duckdb
