#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

    static bool SkipToClose(idx_t& idx, const char* buf, idx_t& len, idx_t& lvl) {
        while (idx < len) {
            if (buf[idx] == '[') {
                if (!SkipToClose(++idx, buf, len, lvl)) {
                    return false;
                }
                lvl++;
                idx++;
            }
            if (buf[idx] == ']') {
                lvl--;
                return true;
            }
            idx++;
        }
        return false;
    }

    static bool SkipToCloseQuotes(idx_t& pos, const char* buf, idx_t& len) {
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

    bool  VectorStringifiedListParser::SplitStringifiedList(const string_t& input, string_t* child_data, idx_t& child_start, Vector& child, bool is_stringlist) {
        idx_t pos = 0;
        const char* buf = input.GetDataUnsafe();
        idx_t len = input.GetSize();
        idx_t lvl = 1;

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
            }
            else if(is_stringlist && (buf[pos] == '"' || buf[pos] == '\'')) {
                SkipToCloseQuotes(pos, buf, len);
            }
            else if (buf[pos] == ','  || buf[pos] == ']') {
                child_data[child_start] = StringVector::AddString(child, buf + start_pos, pos - start_pos);
                if (child_data[child_start].GetSize() >= 4
                        && buf[start_pos] == 'N'
                        && buf[start_pos + 1] == 'U'
                        && buf[start_pos + 2] == 'L'
                        && buf[start_pos + 3] == 'L') {
                    FlatVector::SetNull(child, child_start, true);
                }
                child_start++;

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
        // check if it ends with a ']' (followed by optional whitespace)
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

    idx_t VectorStringifiedListParser::CountParts(const string_t& input) {
        idx_t count = 0;
        idx_t pos = 0;
        const char* buf = input.GetDataUnsafe();
        idx_t len = input.GetSize();

        while (pos < len && StringUtil::CharacterIsSpace(buf[pos])) {
            pos++;
        }
        if (len == pos || buf[pos] != '[') {
            return count;
        }
        pos++;

        idx_t lvl = 1;
        while (pos < len) {
            if (buf[pos] == '[') {
                SkipToClose(++pos, buf, len, lvl);
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
} // namespace duckdb