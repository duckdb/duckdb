#include "duckdb/planner/expression_binder/select_binder.hpp"

namespace duckdb {

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info,
                           case_insensitive_map_t<idx_t> alias_map)
    : BaseSelectBinder(binder, context, node, info, std::move(alias_map)) {
}

SelectBinder::SelectBinder(Binder &binder, ClientContext &context, BoundSelectNode &node, BoundGroupInformation &info)
    : SelectBinder(binder, context, node, info, case_insensitive_map_t<idx_t>()) {
}

} // namespace duckdb
