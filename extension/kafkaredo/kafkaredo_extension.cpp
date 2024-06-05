#define DUCKDB_EXTENSION_MAIN

#include "kafkaredo_extension.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "kafkafs.hpp"

#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
// #include <openssl/opensslv.h>

namespace duckdb {

	inline void KafkaredoScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
			return StringVector::AddString(result, "Kafkaredo " + name.GetString() + " 🐥");
		});
	}
	#if 0
	inline void KafkaredoOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
		auto &name_vector = args.data[0];
		UnaryExecutor::Execute<string_t, string_t>(
			name_vector, result, args.size(),
			[&](string_t name) {
				return StringVector::AddString(result, "Kafkaredo " + name.GetString() +
														 ", my linked OpenSSL version is " +
														 OPENSSL_VERSION_TEXT );;
			});
	}
	#endif
	static void LoadInternal(DatabaseInstance &instance) {
		auto &fs = instance.GetFileSystem();
		fs.RegisterSubSystem(make_uniq<KafkaFileSystem>());

	#if 0
		// Register a scalar function
		auto kafkaredo_scalar_function = ScalarFunction("kafkaredo", {LogicalType::VARCHAR}, LogicalType::VARCHAR, KafkaredoScalarFun);
		  duckdb::Printer::PrintF("loadinternal1: %08X\n", &instance);
		ExtensionUtil::RegisterFunction(instance, kafkaredo_scalar_function);
	  duckdb::Printer::PrintF("loadinternal2: %08X\n", &instance);
		// Register another scalar function
		auto kafkaredo_openssl_version_scalar_function = ScalarFunction("kafkaredo_openssl_version", {LogicalType::VARCHAR},
													LogicalType::VARCHAR, KafkaredoOpenSSLVersionScalarFun);
		  duckdb::Printer::PrintF("loadinternal3: %08X\n", &instance);
		ExtensionUtil::RegisterFunction(instance, kafkaredo_openssl_version_scalar_function);
	#endif
	}

	void KafkaredoExtension::Load(DuckDB &db) {
		LoadInternal(*db.instance);
	}
	std::string KafkaredoExtension::Name() {
		return "kafkaredo";
	}

} // namespace duckdb

extern "C" {

	DUCKDB_EXTENSION_API void kafkaredo_init(duckdb::DatabaseInstance &db) {
		duckdb::DuckDB db_wrapper(db);
		db_wrapper.LoadExtension<duckdb::KafkaredoExtension>();
	}

	DUCKDB_EXTENSION_API const char *kafkaredo_version() {
		return duckdb::DuckDB::LibraryVersion();
	}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
