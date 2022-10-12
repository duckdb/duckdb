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

    struct CountPartOperation {
        idx_t count = 0;
        bool is_stringlist = false;

        void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {
            count++;
        }
    };

    struct SplitStringOperation {
        SplitStringOperation(string_t* child_data, idx_t& child_start, Vector& child, bool is_stringlist) :
            child_data(child_data), child_start(child_start), child(child), is_stringlist(is_stringlist) {}

        string_t* child_data;
        idx_t& child_start;
        Vector& child;
        bool is_stringlist;

        void HandleValue(const char *buf, idx_t start_pos, idx_t pos) {
            child_data[child_start] = StringVector::AddString(child, buf + start_pos, pos - start_pos);
            if (child_data[child_start].GetSize() >= 4
                && buf[start_pos] == 'N'
                && buf[start_pos + 1] == 'U'
                && buf[start_pos + 2] == 'L'
                && buf[start_pos + 3] == 'L') {
                FlatVector::SetNull(child, child_start, true);
            }
            child_start++;
        }
    };

    template<class OP>
    static bool SplitStringifiedListInternal(const string_t& input, OP &state) {
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
            else if(state.is_stringlist && (buf[pos] == '"' || buf[pos] == '\'')) {
                SkipToCloseQuotes(pos, buf, len);
            }
            else if (buf[pos] == ','  || buf[pos] == ']') {
                state.HandleValue(buf, start_pos, pos);
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

    bool VectorStringifiedListParser::SplitStringifiedList(const string_t& input, string_t* child_data, idx_t& child_start, Vector& child, bool is_stringlist) {
        SplitStringOperation state(child_data, child_start, child, is_stringlist);
        return SplitStringifiedListInternal<SplitStringOperation>(input, state);
    }

    idx_t VectorStringifiedListParser::CountParts(const string_t& input) {
        CountPartOperation state;
        SplitStringifiedListInternal<CountPartOperation>(input, state);
        return state.count;
    }
} // namespace duckdb