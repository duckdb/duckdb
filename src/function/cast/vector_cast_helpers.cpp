#include "duckdb/function/cast/vector_cast_helpers.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/stack.hpp"

namespace {

struct StringCastInputState {
public:
	StringCastInputState(const char *buf, idx_t &pos, idx_t &len) : buf(buf), pos(pos), len(len) {
	}

public:
	const char *buf;
	idx_t &pos;
	idx_t &len;
	bool escaped = false;
};

} // namespace

namespace duckdb {

// ------- Helper functions for splitting string nested types  -------
static bool IsNull(StringCastInputState &input_state) {
	auto &buf = input_state.buf;
	auto &pos = input_state.pos;
	if (input_state.pos + 4 != input_state.len) {
		return false;
	}
	return StringUtil::CIEquals(string(buf + pos, buf + pos + 4), "null");
}

inline static void SkipWhitespace(StringCastInputState &input_state) {
	auto &buf = input_state.buf;
	auto &pos = input_state.pos;
	auto &len = input_state.len;
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
		input_state.escaped = false;
	}
}

static bool SkipToCloseQuotes(StringCastInputState &input_state) {
	auto &buf = input_state.buf;
	auto &pos = input_state.pos;
	auto &len = input_state.len;
	auto &escaped = input_state.escaped;

	char quote = buf[pos];
	pos++;

	while (pos < len) {
		if (buf[pos] == '\\') {
			escaped = true;
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

static bool SkipToClose(StringCastInputState &input_state, idx_t &lvl, char close_bracket) {
	auto &idx = input_state.pos;
	auto &buf = input_state.buf;
	auto &len = input_state.len;
	auto &escaped = input_state.escaped;
	idx++;

	vector<char> brackets;
	brackets.push_back(close_bracket);
	while (idx < len) {
		if (!escaped) {
			if (buf[idx] == '"' || buf[idx] == '\'') {
				if (!SkipToCloseQuotes(input_state)) {
					return false;
				}
			} else if (buf[idx] == '{') {
				brackets.push_back('}');
			} else if (buf[idx] == '[') {
				brackets.push_back(']');
				lvl++;
			} else if (buf[idx] == brackets.back()) {
				if (buf[idx] == ']') {
					if (lvl == 0) {
						return false;
					}
					lvl--;
				}
				brackets.pop_back();
				if (brackets.empty()) {
					return true;
				}
			} else if (buf[idx] == '\\') {
				escaped = true;
			}
		} else {
			escaped = false;
		}
		idx++;
	}
	return false;
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

static string_t HandleString(Vector &vec, const char *buf, idx_t start, idx_t end) {
	D_ASSERT(start <= end);
	auto length = end - start;
	auto allocated_string = StringVector::EmptyString(vec, length);
	auto string_data = allocated_string.GetDataWriteable();
	uint32_t copied_count = 0;
	bool escaped = false;

	bool quoted = false;
	char quote_char;
	stack<char> scopes;
	for (idx_t i = 0; i < length; i++) {
		auto current_char = buf[start + i];
		if (!escaped) {
			if (scopes.empty() && current_char == '\\') {
				//! Start of escape
				escaped = true;
				continue;
			}
			if (scopes.empty() && (current_char == '\'' || current_char == '"')) {
				if (quoted && current_char == quote_char) {
					quoted = false;
					//! Skip the ending quote
					continue;
				} else if (!quoted) {
					quoted = true;
					quote_char = current_char;
					//! Skip the starting quote
					continue;
				}
			}
			if (!quoted && !scopes.empty() && current_char == scopes.top()) {
				//! Close scope
				scopes.pop();
			}
			if (!quoted && (current_char == '[' || current_char == '{')) {
				//! New scope
				scopes.push(current_char == '[' ? ']' : '}');
			}
			//! Regular character
			string_data[copied_count++] = current_char;
		} else {
			string_data[copied_count++] = current_char;
			escaped = false;
		}
	}
	return string_t((const char *)string_data, copied_count); // NOLINT
}

// ------- LIST SPLIT -------
struct SplitStringListOperation {
public:
	SplitStringListOperation(string_t *child_data, idx_t &entry_count, Vector &child)
	    : child_data(child_data), entry_count(entry_count), child(child) {
	}

public:
	void HandleValue(const char *buf, idx_t start, idx_t end) {
		StringCastInputState temp_state(buf, start, end);
		if (IsNull(temp_state)) {
			FlatVector::SetNull(child, entry_count, true);
			entry_count++;
			return;
		}
		child_data[entry_count] = HandleString(child, buf, start, end);
		entry_count++;
	}

private:
	string_t *child_data;
	idx_t &entry_count;
	Vector &child;
};

template <class OP>
static bool SplitStringListInternal(const string_t &input, OP &state) {
	const char *buf = input.GetData();
	idx_t len = input.GetSize();
	idx_t lvl = 1;
	idx_t pos = 0;

	StringCastInputState input_state(buf, pos, len);

	SkipWhitespace(input_state);
	if (pos == len || buf[pos] != '[') {
		//! Does not have a valid list start
		return false;
	}

	//! Skip the '['
	pos++;
	SkipWhitespace(input_state);
	optional_idx start_pos;
	idx_t end_pos;
	bool seen_value = false;
	while (pos < len) {
		if (pos == len) {
			return false;
		}
		bool set_escaped = false;

		if (buf[pos] == '[') {
			if (!start_pos.IsValid()) {
				start_pos = pos;
			}
			//! Start of a LIST
			if (!input_state.escaped) {
				lvl++;
				if (!SkipToClose(input_state, lvl, ']')) {
					return false;
				}
			}
			end_pos = pos;
		} else if ((buf[pos] == '"' || buf[pos] == '\'')) {
			if (!start_pos.IsValid()) {
				start_pos = pos;
			}
			if (!input_state.escaped) {
				if (!SkipToCloseQuotes(input_state)) {
					return false;
				}
			}
			end_pos = pos;
		} else if (buf[pos] == '{') {
			if (!start_pos.IsValid()) {
				start_pos = pos;
			}
			//! Start of a STRUCT
			if (!input_state.escaped) {
				idx_t struct_lvl = 0;
				if (!SkipToClose(input_state, struct_lvl, '}')) {
					return false;
				}
			}
			end_pos = pos;
		} else if (buf[pos] == ',' || buf[pos] == ']') {
			if (buf[pos] != ']' || start_pos.IsValid() || seen_value) {
				if (!start_pos.IsValid()) {
					state.HandleValue(buf, 0, 0);
				} else {
					auto start = start_pos.GetIndex();
					auto end = (end_pos + 1) - start;
					auto substr = std::string(buf + start, end);
					state.HandleValue(buf, start, end_pos + 1);
				}
				seen_value = true;
			}
			if (buf[pos] == ']') {
				if (lvl == 0) {
					return false;
				}
				lvl--;
				break;
			}
			start_pos = optional_idx();
		} else if (buf[pos] == '\\') {
			if (!start_pos.IsValid()) {
				start_pos = pos;
			}
			if (!input_state.escaped) {
				set_escaped = true;
			}
		} else if (!StringUtil::CharacterIsSpace(buf[pos])) {
			if (!start_pos.IsValid()) {
				start_pos = pos;
			}
			end_pos = pos;
		}
		input_state.escaped = set_escaped;
		pos++;
		SkipWhitespace(input_state);
	}
	pos++;
	SkipWhitespace(input_state);
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
		StringCastInputState temp_state(buf, start_pos, pos);
		if (IsNull(temp_state)) {
			FlatVector::SetNull(varchar_val, child_start, true);
			child_start++;
			return false;
		}
		child_key_data[child_start] = HandleString(varchar_key, buf, start_pos, pos);
		return true;
	}

	void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {
		StringCastInputState temp_state(buf, start_pos, pos);
		if (IsNull(temp_state)) {
			FlatVector::SetNull(varchar_val, child_start, true);
			child_start++;
			return;
		}
		child_val_data[child_start] = HandleString(varchar_val, buf, start_pos, pos);
		child_start++;
	}
};

template <class OP>
static bool SplitStringMapInternal(const string_t &input, OP &state) {
	const char *buf = input.GetData();
	idx_t len = input.GetSize();
	idx_t pos = 0;
	StringCastInputState input_state(buf, pos, len);
	idx_t lvl = 0;

	SkipWhitespace(input_state);
	if (pos == len || buf[pos] != '{') {
		return false;
	}
	pos++;
	SkipWhitespace(input_state);
	if (pos == len) {
		return false;
	}

	while (pos < len) {
		optional_idx start_pos;
		idx_t end_pos;
		while (pos < len && (buf[pos] != '=' || input_state.escaped)) {
			bool set_escaped = false;
			if (buf[pos] == '"' || buf[pos] == '\'') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					if (!SkipToCloseQuotes(input_state)) {
						return false;
					}
				}
				end_pos = pos;
			} else if (buf[pos] == '{') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					if (!SkipToClose(input_state, lvl, '}')) {
						return false;
					}
				}
				end_pos = pos;
			} else if (buf[pos] == '[') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					lvl++;
					if (!SkipToClose(input_state, lvl, ']')) {
						return false;
					}
				}
				end_pos = pos;
			} else if (buf[pos] == '\\') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					set_escaped = true;
				}
			} else if (!StringUtil::CharacterIsSpace(buf[pos])) {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				end_pos = pos;
			}
			input_state.escaped = set_escaped;
			pos++;
		}
		if (pos == len) {
			return false;
		}
		if (!start_pos.IsValid()) {
			//! Key can not be empty
			return false;
		}
		auto key_substr = std::string(buf + start_pos.GetIndex(), buf + end_pos + 1);
		if (!state.HandleKey(buf, start_pos.GetIndex(), end_pos + 1)) {
			return false;
		}
		start_pos = optional_idx();
		pos++;
		SkipWhitespace(input_state);
		while (pos < len && ((buf[pos] != ',' && buf[pos] != '}') || input_state.escaped)) {
			if (buf[pos] == '"' || buf[pos] == '\'') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					if (!SkipToCloseQuotes(input_state)) {
						return false;
					}
				}
				end_pos = pos;
			} else if (buf[pos] == '{') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					if (!SkipToClose(input_state, lvl, '}')) {
						return false;
					}
				}
				end_pos = pos;
			} else if (buf[pos] == '[') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					lvl++;
					if (!SkipToClose(input_state, lvl, ']')) {
						return false;
					}
				}
				end_pos = pos;
			} else if (buf[pos] == '\\') {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				if (!input_state.escaped) {
					input_state.escaped = true;
				}
			} else if (!StringUtil::CharacterIsSpace(buf[pos])) {
				if (!start_pos.IsValid()) {
					start_pos = pos;
				}
				end_pos = pos;
			}
			pos++;
		}
		if (pos == len) {
			return false;
		}
		if (!start_pos.IsValid()) {
			//! Value is empty
			state.HandleValue(buf, 0, 0);
		} else {
			auto value_substr = std::string(buf + start_pos.GetIndex(), buf + end_pos + 1);
			state.HandleValue(buf, start_pos.GetIndex(), end_pos + 1);
		}
		if (buf[pos] == '}') {
			break;
		}
		pos++;
		SkipWhitespace(input_state);
	}
	pos++;
	SkipWhitespace(input_state);
	return (pos == len && lvl == 0);
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

static bool FindValueStruct(StringCastInputState &input_state, Vector &varchar_child, idx_t &row_idx,
                            ValidityMask &child_mask) {
	auto start_pos = input_state.pos;
	idx_t lvl = 0;

	auto &len = input_state.len;
	auto &pos = input_state.pos;
	auto &buf = input_state.buf;
	while (pos < len) {
		if (buf[pos] == '"' || buf[pos] == '\'') {
			SkipToCloseQuotes(input_state);
		} else if (buf[pos] == '{') {
			SkipToClose(input_state, lvl, '}');
		} else if (buf[pos] == '[') {
			SkipToClose(input_state, lvl, ']');
		} else if (buf[pos] == ',' || buf[pos] == '}') {
			// TODO: start_pos at first non-whitespace character
			StringCastInputState temp_state(buf, start_pos, pos);
			if (IsNull(temp_state)) {
				FlatVector::SetNull(varchar_child, row_idx, true);
				return true;
			}
			// TODO: copy the unescaped portion of the string
			FlatVector::GetData<string_t>(varchar_child)[row_idx] =
			    StringVector::AddString(varchar_child, buf + start_pos, pos - start_pos);
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

	StringCastInputState input_state(buf, pos, len);

	SkipWhitespace(input_state);
	if (pos == len || buf[pos] != '{') {
		return false;
	}
	pos++;
	SkipWhitespace(input_state);
	if (buf[pos] == '}') {
		pos++;
	} else {
		while (pos < len) {
			auto key_start = pos;
			if (!FindKeyStruct(buf, len, pos)) {
				return false;
			}
			// TODO: process the string
			// auto key_end = StringTrim(buf, key_start, pos);
			auto key_end = pos;
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
			pos++;
			SkipWhitespace(input_state);
			if (!FindValueStruct(input_state, *varchar_vectors[child_idx], row_idx, child_masks[child_idx].get())) {
				return false;
			}
			pos++;
			SkipWhitespace(input_state);
		}
	}
	SkipWhitespace(input_state);
	return (pos == len);
}

} // namespace duckdb
