#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret_manager.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/serializer/buffered_file_reader.hpp"


namespace duckdb {

struct DuckDBSecretsToFileBindData : public FunctionData {
	explicit DuckDBSecretsToFileBindData(string file_p) : file(file_p) {};
	string file;

public:
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<DuckDBSecretsToFileBindData>(file);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<DuckDBSecretsToFileBindData>();
		return file == other.file;
	}
};

static unique_ptr<FunctionData> DuckDBSecretsToFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw InvalidInputException("DuckDBSecretsToFileBind");
	}

	names.emplace_back("success");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<DuckDBSecretsToFileBindData>(input.inputs[0].ToString());
}


static void DuckDBSecretsToFileFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &secret_manager = context.db->config.secret_manager;
	auto &secrets = secret_manager->AllSecrets();
	auto& bind_data = data_p.bind_data->Cast<DuckDBSecretsToFileBindData>();

	auto file_writer = BufferedFileWriter(*context.db->config.file_system, bind_data.file);
	auto serializer = BinarySerializer(file_writer);
	for (auto& secret : secrets) {
		serializer.Begin();
		secret->Serialize(serializer);
		serializer.End();
	}
	file_writer.Flush();

	return;
}

static void DuckDBSecretsFromFileFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &secret_manager = context.db->config.secret_manager;
	auto& bind_data = data_p.bind_data->Cast<DuckDBSecretsToFileBindData>();

	auto file_reader = BufferedFileReader(*context.db->config.file_system, bind_data.file.c_str());


	while(!file_reader.Finished()) {
		BinaryDeserializer deserializer(file_reader);
		deserializer.Begin();
		shared_ptr<RegisteredSecret> deserialized_secret = secret_manager->DeserializeSecret(deserializer);
		secret_manager->RegisterSecret(deserialized_secret, OnCreateConflict::ERROR_ON_CONFLICT);

		deserializer.End();
	}

	return;
}

void DuckDBSecretsToFileFun::RegisterFunction(BuiltinFunctions &set) {
	TableFunctionSet function_to("duckdb_secrets_to_file");
	function_to.AddFunction(TableFunction({LogicalType::VARCHAR}, DuckDBSecretsToFileFunction, DuckDBSecretsToFileBind));
	set.AddFunction(function_to);

	TableFunctionSet function_from("duckdb_secrets_from_file");
	function_from.AddFunction(TableFunction({LogicalType::VARCHAR}, DuckDBSecretsFromFileFunction, DuckDBSecretsToFileBind));
	set.AddFunction(function_from);
}

} // namespace duckdb
