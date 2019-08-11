#include "function/aggregate/distributive_functions.hpp"
#include "common/exception.hpp"
#include "common/types/null_value.hpp"
#include "common/vector_operations/vector_operations.hpp"
#include <string>

using namespace std;

namespace duckdb {

typedef const char* string_agg_state_t;

SQLType string_agg_get_return_type(vector<SQLType> &arguments) {
    if (arguments.size() != 2)
        return SQLTypeId::INVALID;

    for ( index_t i = 0; i < arguments.size(); ++i) {
        switch (arguments[i].id) {
        case SQLTypeId::SQLNULL:
        case SQLTypeId::VARCHAR:
            break;
        default:
        return SQLTypeId::INVALID;
        }
    }

    return SQLTypeId::VARCHAR;
}

void string_agg_update(Vector inputs[], index_t input_count, Vector &state) {
    assert(input_count == 2);
    auto& strs = inputs[0];
    auto& seps = inputs[1];
    assert(strs.type == TypeId::VARCHAR);
    assert(seps.type == TypeId::VARCHAR);

    auto str_data = (const char **)strs.data;
    auto sep_data = (const char **)seps.data;

    //  Share a reusable buffer for the block
    std::string buffer;

    VectorOperations::Exec(state, [&](index_t i, index_t k) {
        if (strs.nullmask[i] || seps.nullmask[i]) {
            return;
        }

        auto state_ptr = (string_agg_state_t*) ((data_ptr_t *)state.data)[i];
        auto str = str_data[i];
        auto sep = sep_data[i];
        if (IsNullValue(*state_ptr)) {
            *state_ptr = strs.string_heap.AddString(str);
        } else {
            buffer = *state_ptr;
            buffer += sep;
            buffer += str;
            *state_ptr = strs.string_heap.AddString(buffer.c_str());
        }
    });
}

} // namespace duckdb
