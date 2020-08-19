#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/merge_join.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"

using namespace std;

namespace duckdb {

template<class T, class OP>
idx_t merge_join_complex_lt(ScalarMergeInfo &l, ScalarMergeInfo &r) {
    if (r.pos >= r.order.count) {
        return 0;
    }
    auto ldata = (T *)l.order.vdata.data;
    auto rdata = (T *)r.order.vdata.data;
    auto &lorder = l.order.order;
    auto &rorder = r.order.order;
    idx_t result_count = 0;
    while (true) {
        if (l.pos < l.order.count) {
            auto lidx = lorder.get_index(l.pos);
            auto ridx = rorder.get_index(r.pos);
            auto dlidx = l.order.vdata.sel->get_index(lidx);
            auto dridx = r.order.vdata.sel->get_index(ridx);
            if (OP::Operation(ldata[dlidx], rdata[dridx])) {
                // left side smaller: found match
                l.result.set_index(result_count, lidx);
                r.result.set_index(result_count, ridx);
                result_count++;
                // move left side forward
                l.pos++;
                if (result_count == STANDARD_VECTOR_SIZE) {
                    // out of space!
                    break;
                }
                continue;
            }
        }
        // right side smaller or equal, or left side exhausted: move
        // right pointer forward reset left side to start
        l.pos = 0;
        r.pos++;
        if (r.pos == r.order.count) {
            break;
        }
    }
    return result_count;

}

template <class T> idx_t MergeJoinComplex::LessThan::Operation(ScalarMergeInfo &l, ScalarMergeInfo &r) {
	return merge_join_complex_lt<T, duckdb::LessThan>(l, r);
}

template <class T> idx_t MergeJoinComplex::LessThanEquals::Operation(ScalarMergeInfo &l, ScalarMergeInfo &r) {
    return merge_join_complex_lt<T, duckdb::LessThanEquals>(l, r);
}

INSTANTIATE_MERGEJOIN_TEMPLATES(MergeJoinComplex, LessThan, ScalarMergeInfo, ScalarMergeInfo)
INSTANTIATE_MERGEJOIN_TEMPLATES(MergeJoinComplex, LessThanEquals, ScalarMergeInfo, ScalarMergeInfo)

}
