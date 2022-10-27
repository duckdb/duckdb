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

static bool SkipToCloseStructOrList(idx_t &idx, const char *buf, idx_t &len, char close_bracket) {
	char bracket = buf[idx];
	idx++;

	while (idx < len) {
		if (buf[idx] == bracket) {
			if (!SkipToCloseStructOrList(idx, buf, len, close_bracket)) {
				return false;
			}
			idx++;
		}
		if (buf[idx] == '"' || buf[idx] == '\'') {
			SkipToCloseQuotes(idx, buf, len);
		}
		if (buf[idx] == close_bracket) {
			return true;
		}
		idx++;
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
		if (buf[idx] == '{') {
			SkipToCloseStructOrList(idx, buf, len, '}');
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
		} else if (buf[pos] == '{') {
			SkipToCloseStructOrList(pos, buf, len, '}');
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

//------------------- struct splitting ----------------------

static bool FindKey(const char *buf, idx_t len, idx_t &pos) {
	while (pos < len) {
		if (buf[pos] == ':') {
			return true;
		}
		pos++;
	}
	return false;
}

static bool FindValue(const char *buf, idx_t len, idx_t &pos, Vector &varchar_child, idx_t &row_idx) {
	auto start_pos = pos;
	while (pos < len) {
		if (buf[pos] == '"' || buf[pos] == '\'') {
			SkipToCloseQuotes(pos, buf, len);
		} else if (buf[pos] == '{') {
			SkipToCloseStructOrList(pos, buf, len, '}');
		} else if (buf[pos] == '[') {
			SkipToCloseStructOrList(pos, buf, len, ']');
		} else if (buf[pos] == ',' || buf[pos] == '}') {
			idx_t trailing_whitespace = 0;
			while (StringUtil::CharacterIsSpace(buf[pos - trailing_whitespace - 1])) {
				trailing_whitespace++;
			}
			if (buf[start_pos] == '"' && buf[pos - trailing_whitespace - 1] == '"') {
				start_pos++;
				trailing_whitespace++;
			}
			FlatVector::GetData<string_t>(varchar_child)[row_idx] = StringVector::AddString(varchar_child, buf + start_pos, pos - start_pos - trailing_whitespace);
            return true;
		}
		pos++;
	}
	return false;
}

//bool VectorStringifiedStructParser::SplitStruct(string_t &input, std::vector<Vector> &varchar_vectors, idx_t &row_idx,
//                                                std::vector<string_t> &child_names) {

bool VectorStringifiedStructParser::SplitStruct(string_t &input, std::vector<std::unique_ptr<Vector>> &varchar_vectors, idx_t &row_idx,
                                                string_map_t<idx_t> &child_names, std::vector<ValidityMask*> &child_masks) {
	const char *buf = input.GetDataUnsafe();
	idx_t len = input.GetSize();
	idx_t pos = 0;
	idx_t child_idx = 0;
    idx_t total_elements = 0;

	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}
	if (pos == len || buf[pos] != '{') {
		return false;
	}

	pos++;
	while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
		pos++;
	}

	while (pos < len) {
        auto key_start = pos;

        if (!FindKey(buf, len, pos)) {
            return false;
        }
        auto key_end = pos;

        idx_t trailing_whitespace = 0;
        while (StringUtil::CharacterIsSpace(buf[key_end - trailing_whitespace - 1])) {
            trailing_whitespace++;
        }
        if ((buf[key_start] == '"' && buf[pos - trailing_whitespace - 1] == '"') ||
            (buf[key_start] == '\'' && buf[pos - trailing_whitespace - 1] == '\'')) {
            key_start++;
            trailing_whitespace++;
        }
        key_end = key_end - trailing_whitespace;
        auto key_len = key_end - key_start;

        string_t found_key(buf + key_start, key_end - key_start);

        auto it = child_names.find(found_key);
        if(it != child_names.end()){
            child_idx = it->second;
        } else{
            return false; // a key was entered that does not correspond with any of the target keynames
        }


//        while(child_idx < child_names.size()){
//            const char *child_buf = child_names[child_idx].GetDataUnsafe();
//            if (child_names[child_idx].GetSize() != key_len){
//                child_idx++;
//                continue; // key does not match
//            }
//            idx_t i = 0;
//            for (; i < key_len; i++){
//                if (child_buf[i] != buf[key_start + i]){
//                    break;
//                }
//            }
//            if (key_start + i == key_end){ // key matches
//                break;
//            }
//            if (varchar_vectors[child_idx].GetVectorType() == VectorType::CONSTANT_VECTOR){
//                ConstantVector::SetNull(varchar_vectors[child_idx], true);
//            } else {
//                FlatVector::SetNull(varchar_vectors[child_idx], row_idx, true);
//            }
//            child_idx++;
//        }

		pos++;
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
		if (!FindValue(buf, len, pos, *varchar_vectors[child_idx], row_idx)) {
			return false;
		}
        child_masks[child_idx]->SetValid(row_idx);
		pos++;
		while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
			pos++;
		}
        total_elements++;
	}

//    if (total_elements < varchar_vectors.size()) { // missing values, so these need to be set to null
//        for(idx_t i = 0; i < varchar_vectors.size(); i++){
//            auto it = set_keys.find(i);
//            if(it == set_keys.end()){
//                if (varchar_vectors[i]->GetVectorType() == VectorType::CONSTANT_VECTOR){
//                    ConstantVector::SetNull(*varchar_vectors[i], true);
//                } else {
//                    FlatVector::SetNull(*varchar_vectors[i], row_idx, true);
//                }
//                total_elements++;
//            }
//        }
//    }
//    if (total_elements != varchar_vectors.size()) { // string not splittable into correct number of values
//        return false;
//    }
	while (pos < len) {
		if (!StringUtil::CharacterIsSpace(buf[pos])) {
			return false;
		}
		pos++;
	}
	return true;
}

} // namespace duckdb
