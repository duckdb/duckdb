#include "duckdb/function/cast/vector_cast_helpers.hpp"

namespace duckdb {

    VectorSplitStringifiedList::VectorSplitStringifiedList(string input) : input(input) {
        Split();
    }

    void VectorSplitStringifiedList::Split() {
        ParserState ps = INITIAL;
        int start = 0;

        for (idx_t i = 0; i < input.size(); i++) {
            switch (ps) {
                case INITIAL:
                    switch (input[i]) {
                        case '[':
                            ps = BRACKETS;
                            start = i + 1;
                            break;
                        default:
                            break;
                    }
                    break;
                case BRACKETS:
                    switch (input[i]) {
                        case '[':
                            break;
                        case ']':
                            parts.push_back(input.substr(start, i - start));
                            ps = INITIAL;
                            break;
                        case ',':
                            parts.push_back(input.substr(start, i - start));
                            start = i + 1;
                            break;
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
    }

} // namespace duckdb