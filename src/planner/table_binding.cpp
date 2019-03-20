#include "planner/table_binding.hpp"
#include "planner/tableref/bound_basetableref.hpp"

using namespace duckdb;
using namespace std;

DummyTableBinding::DummyTableBinding(vector<ColumnDefinition> &columns) :
	Binding(BindingType::DUMMY, 0) {
	for (auto &it : columns) {
		bound_columns[it.name] = &it;
	}
}

TableBinding::TableBinding(BoundBaseTableRef *bound) :
	Binding(BindingType::TABLE, bound->bind_index), bound(bound) {
}

SubqueryBinding::SubqueryBinding(SubqueryRef &subquery_, size_t index)
    : Binding(BindingType::SUBQUERY, index), subquery(subquery_.subquery.get()) {
	auto &select_list = subquery->GetSelectList();
	if (subquery_.column_name_alias.size() > 0) {
		assert(subquery_.column_name_alias.size() == select_list.size());
		for (auto &name : subquery_.column_name_alias) {
			name_map[name] = names.size();
			names.push_back(name);
		}
	} else {
		for (auto &entry : select_list) {
			auto name = entry->GetName();
			name_map[name] = names.size();
			names.push_back(name);
		}
	}
}

SubqueryBinding::SubqueryBinding(QueryNode *select_, size_t index)
    : Binding(BindingType::SUBQUERY, index), subquery(select_) {
	auto &select_list = subquery->GetSelectList();
	for (auto &entry : select_list) {
		auto name = entry->GetName();
		name_map[name] = names.size();
		names.push_back(name);
	}
}

TableFunctionBinding::TableFunctionBinding(TableFunctionCatalogEntry *function, size_t index)
	: Binding(BindingType::TABLE_FUNCTION, index), function(function) {
}
