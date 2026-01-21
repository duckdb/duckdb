//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/filter/extension_table_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_extension.hpp"
#include "duckdb/planner/table_filter_state.hpp"

namespace duckdb {

class ExtensionTableFilter : public TableFilter {
public:
	static constexpr const TableFilterType TYPE = TableFilterType::EXTENSION;

public:
	explicit ExtensionTableFilter(TableFilterType type_p = TableFilterType::EXTENSION) : TableFilter(type_p) {
	}

	FilterPropagateResult CheckStatistics(BaseStatistics &stats) const override {
		return FilterPropagateResult::NO_PRUNING_POSSIBLE;
	}

	string ToString(const string &column_name) const override {
		return "ExtensionTableFilter";
	}

	unique_ptr<TableFilter> Copy() const override {
		throw InternalException("ExtensionTableFilter::Copy() not implemented");
	}

	unique_ptr<Expression> ToExpression(const Expression &column) const override {
		throw InternalException("ExtensionTableFilter::ToExpression() not implemented");
	}

	void Serialize(Serializer &serializer) const override {
		throw InternalException("ExtensionTableFilter::Serialize() not implemented");
	}

	static unique_ptr<TableFilter> Deserialize(Deserializer &deserializer);

public:
	virtual unique_ptr<TableFilterState> InitializeState(ClientContext &context) const {
		return make_uniq<ExtensionFilterState>();
	}

	virtual idx_t Filter(Vector &keys_v, SelectionVector &sel, idx_t &approved_tuple_count,
	                     TableFilterState &state) const {
		throw InternalException("ExtensionTableFilter::Filter() not implemented");
	}
};

} // namespace duckdb
