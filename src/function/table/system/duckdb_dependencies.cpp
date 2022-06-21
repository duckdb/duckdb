#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DependencyInformation {
	CatalogEntry *object;
	CatalogEntry *dependent;
	DependencyType type;
};

struct DuckDBDependenciesData : public GlobalTableFunctionState {
	DuckDBDependenciesData() : offset(0) {
	}

	vector<DependencyInformation> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBDependenciesBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("classid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("objid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("objsubid");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("refclassid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("refobjid");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("refobjsubid");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("deptype");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBDependenciesInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_unique<DuckDBDependenciesData>();

	// scan all the schemas and collect them
	auto &catalog = Catalog::GetCatalog(context);
	auto &dependency_manager = catalog.GetDependencyManager();
	dependency_manager.Scan([&](CatalogEntry *obj, CatalogEntry *dependent, DependencyType type) {
		DependencyInformation info;
		info.object = obj;
		info.dependent = dependent;
		info.type = type;
		result->entries.push_back(info);
	});

	return move(result);
}

void DuckDBDependenciesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DuckDBDependenciesData &)*data_p.global_state;
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;
	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];

		// return values:
		// classid, LogicalType::BIGINT
		output.SetValue(0, count, Value::BIGINT(0));
		// objid, LogicalType::BIGINT
		output.SetValue(1, count, Value::BIGINT(entry.object->oid));
		// objsubid, LogicalType::INTEGER
		output.SetValue(2, count, Value::INTEGER(0));
		// refclassid, LogicalType::BIGINT
		output.SetValue(3, count, Value::BIGINT(0));
		// refobjid, LogicalType::BIGINT
		output.SetValue(4, count, Value::BIGINT(entry.dependent->oid));
		// refobjsubid, LogicalType::INTEGER
		output.SetValue(5, count, Value::INTEGER(0));
		// deptype, LogicalType::VARCHAR
		string dependency_type_str;
		switch (entry.type) {
		case DependencyType::DEPENDENCY_REGULAR:
			dependency_type_str = "n";
			break;
		case DependencyType::DEPENDENCY_AUTOMATIC:
			dependency_type_str = "a";
			break;
		default:
			throw NotImplementedException("Unimplemented dependency type");
		}
		output.SetValue(6, count, Value(dependency_type_str));

		data.offset++;
		count++;
	}
	output.SetCardinality(count);
}

void DuckDBDependenciesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_dependencies", {}, DuckDBDependenciesFunction, DuckDBDependenciesBind,
	                              DuckDBDependenciesInit));
}

} // namespace duckdb
