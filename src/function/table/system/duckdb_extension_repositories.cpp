#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_repository_manager.hpp"

namespace duckdb {

struct DuckDBExtensionRepositoriesData : public GlobalTableFunctionState {
	DuckDBExtensionRepositoriesData() : offset(0) {
	}

	vector<ExtensionRepository> entries;
	idx_t offset;
};

static unique_ptr<FunctionData> DuckDBExtensionRepositoriesBind(ClientContext &context, TableFunctionBindInput &input,
                                                                vector<LogicalType> &return_types,
                                                                vector<Identifier> &names) {
	names.emplace_back("repository_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("prefix");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("type");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("key_fingerprint");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("public_key");
	return_types.emplace_back(LogicalType::VARCHAR);

	return nullptr;
}

unique_ptr<GlobalTableFunctionState> DuckDBExtensionRepositoriesInit(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
	auto result = make_uniq<DuckDBExtensionRepositoriesData>();

	auto &db = DatabaseInstance::GetDatabase(context);
	// the built-in repositories, followed by the trusted repositories that were added by the user
	for (auto &name : ExtensionRepository::GetKnownRepositoryNames()) {
		ExtensionRepository repository;
		ExtensionRepository::TryGetKnownRepository(name, repository);
		result->entries.push_back(std::move(repository));
	}
	for (auto &repository : ExtensionRepositoryManager::GetRepositories(db, FileSystem::GetLocal(db))) {
		result->entries.push_back(repository);
	}
	return std::move(result);
}

void DuckDBExtensionRepositoriesFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<DuckDBExtensionRepositoriesData>();
	if (data.offset >= data.entries.size()) {
		// finished returning values
		return;
	}
	idx_t count = 0;

	// repository_name, VARCHAR
	auto &name = output.data[0];
	// prefix, VARCHAR
	auto &prefix = output.data[1];
	// type, VARCHAR
	auto &type = output.data[2];
	// key_fingerprint, VARCHAR
	auto &key_fingerprint = output.data[3];
	// public_key, VARCHAR
	auto &public_key = output.data[4];

	while (data.offset < data.entries.size() && count < STANDARD_VECTOR_SIZE) {
		auto &entry = data.entries[data.offset++];
		name.Append(Value(entry.name));
		prefix.Append(Value(entry.path));
		type.Append(Value(EnumUtil::ToString(entry.type)));
		// only user provided repositories have a key of their own - the core and community keys are built in
		if (entry.public_key.empty()) {
			key_fingerprint.Append(Value(LogicalType::VARCHAR));
			public_key.Append(Value(LogicalType::VARCHAR));
		} else {
			key_fingerprint.Append(Value(ExtensionRepositoryManager::GetPublicKeyFingerprint(entry.public_key)));
			public_key.Append(Value(entry.public_key));
		}
		count++;
	}
}

void DuckDBExtensionRepositoriesFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("duckdb_extension_repositories", {}, DuckDBExtensionRepositoriesFunction,
	                              DuckDBExtensionRepositoriesBind, DuckDBExtensionRepositoriesInit));
}

} // namespace duckdb
