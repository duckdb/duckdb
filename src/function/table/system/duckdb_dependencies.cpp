#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/duck_catalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct DependencyInformation {
	DependencyInformation(CatalogEntry &object, CatalogEntry &dependent, const DependencyDependentFlags &flags)
	    : object(object), dependent(dependent), flags(flags) {
	}

	CatalogEntry &object;
	CatalogEntry &dependent;
	DependencyDependentFlags flags;
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
	auto result = make_uniq<DuckDBDependenciesData>();

	// scan all the schemas and collect them
	auto &catalog = Catalog::GetCatalog(context, INVALID_CATALOG);
	auto dependency_manager = catalog.GetDependencyManager();
	if (dependency_manager) {
		dependency_manager->Scan(
		    context, [&](CatalogEntry &obj, CatalogEntry &dependent, const DependencyDependentFlags &flags) {
			    result->entries.emplace_back(obj, dependent, flags);
		    });
	}

	return std::move(result);
}

void DuckDBDependenciesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBDependenciesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	// start returning values
	// either fill up the chunk or return all the remaining columns
	idx_t count = 0;

	// classid, LogicalType::BIGINT
	auto &classid = output.data[0];
	// objid, LogicalType::BIGINT
	auto &objid = output.data[1];
	// objsubid, LogicalType::INTEGER
	auto &objsubid = output.data[2];
	// refclassid, LogicalType::BIGINT
	auto &refclassid = output.data[3];
	// refobjid, LogicalType::BIGINT
	auto &refobjid = output.data[4];
	// refobjsubid, LogicalType::INTEGER
	auto &refobjsubid = output.data[5];
	// deptype, LogicalType::VARCHAR
	auto &deptype = output.data[6];

	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset];

		classid.Append(Value::BIGINT(0));
		objid.Append(Value::BIGINT(NumericCast<int64_t>(entry.object.oid)));
		objsubid.Append(Value::INTEGER(0));
		refclassid.Append(Value::BIGINT(0));
		refobjid.Append(Value::BIGINT(NumericCast<int64_t>(entry.dependent.oid)));
		refobjsubid.Append(Value::INTEGER(0));
		string dependency_type_str;
		if (entry.flags.IsBlocking()) {
			dependency_type_str = "n";
		} else {
			dependency_type_str = "a";
		}
		deptype.Append(Value(dependency_type_str));

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
