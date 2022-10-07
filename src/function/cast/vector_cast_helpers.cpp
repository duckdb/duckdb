#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

    VectorSplitStringifiedList::VectorSplitStringifiedList(const string_t& input)
        : buf(input.GetDataUnsafe()), len(input.GetSize()),  is_valid(true) {
//        RemoveWhitespace();
//        Split();
    }

    // currently removes whitespace before each element.
    // Possible optimization?: remove whitespace between element and comma to pass smaller strings to next casting functions
    void  VectorSplitStringifiedList::Split() {
        idx_t pos = 0;

        while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
            pos++;
        }
        if (pos == len) { // replace input.empty()
            is_valid = false;
            return;
        }
        if (buf[pos] != '[') {
            is_valid = false;
            return;
        }
        pos++;

        while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
            pos++;
        }
        idx_t start_pos = pos;
        while (pos < len) {
            if (buf[pos] == '[') {
                if (!SkipToClose(++pos)) {
                    is_valid = false;
                    return;
                }
            }
            else if (buf[pos] == ',') {
                parts.emplace_back(buf + start_pos, pos - start_pos);
                while (pos + 1<len && StringUtil::CharacterIsSpace(buf[pos + 1])) {
                    pos++;
                }
                start_pos = pos + 1;
            } else if (buf[pos] == ']') {
                parts.emplace_back(buf + start_pos, pos - start_pos);
                break;
            }
            pos++;
        }
        // check if it ends with a ']' (followed by optional whitespace)
        pos++;
        while (pos<len) {
            if (!StringUtil::CharacterIsSpace(buf[pos])) {
                is_valid = false;
            }
            pos++;
        }



//        int start = 1;
//        for (idx_t i = 1; i < input.size(); i++) {
//            if (input[i] == '[') {
//                SkipToClose(++i);
//            }
//            else if (input[i] == ',' || input[i] == ']') {
//                parts.push_back(input.substr(start, i - start));
//                start = i + 1;
//            }
//        }
    }

    bool VectorSplitStringifiedList::SkipToClose(idx_t& idx) {
        while (idx < len) {
            if (buf[idx] == '[') {
                if (!SkipToClose(++idx)){
                    return false;
                }
                idx++;
            }
            if (buf[idx] == ']') {
                return true;
            }
            idx++;
        }
        return false;
    }

    idx_t VectorSplitStringifiedList::CountParts() {
        idx_t count = 0;
        idx_t pos = 0;

        while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
            pos++;
        }
        if (len == pos) { // replace input.empty()
            return count;
        }
        if (buf[pos] != '[') {
            return count;
        }
        pos++;

        while (pos < len) {
            if (buf[pos] == '[') {
                SkipToClose(++pos);
            }
            else if (buf[pos] == ',') {
                count++;
            } else if (buf[pos] == ']') {
                return ++count;
            }
            pos++;
        }
        return count;
    }

//    void VectorSplitStringifiedList::RemoveWhitespace() {
//        input.erase(remove_if(input.begin(), input.end(), isspace), input.end());
//        //printf("trimmed: |%s|\n", input.c_str());
//    }
} // namespace duckdb