#define DUCKDB_EXTENSION_MAIN
#include "hash-extension.hpp"

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "xxhash.h"

namespace duckdb {

template <class OP>
struct HashFunctionWrapper {
	template <class INPUT_TYPE, class RETURN_TYPE>
	static inline RETURN_TYPE Operation(INPUT_TYPE input) {
		return OP::template Operation<RETURN_TYPE>((data_ptr_t)&input, sizeof(INPUT_TYPE));
	}

	template <>
	inline uint32_t Operation(string_t input) {
		return OP::template Operation<uint32_t>((data_ptr_t)input.GetDataUnsafe(), input.GetSize());
	}

	template <>
	inline uint64_t Operation(string_t input) {
		return OP::template Operation<uint64_t>((data_ptr_t)input.GetDataUnsafe(), input.GetSize());
	}
};

struct XXHash {
	template <class RETURN_TYPE>
	static inline RETURN_TYPE Operation(data_ptr_t input, idx_t size) {
		throw NotImplementedException("RETURN_TYPE for XXHash");
	}

	template <>
	inline uint32_t Operation(data_ptr_t input, idx_t size) {
		return XXH32(input, size, 0);
	}

	template <>
	inline uint64_t Operation(data_ptr_t input, idx_t size) {
		return XXH64(input, size, 0);
	}
};

template <class OP, class INPUT_TYPE, class RETURN_TYPE>
static void HashFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<INPUT_TYPE, RETURN_TYPE>(args.data[0], result, args.size(), [](INPUT_TYPE input) {
		return HashFunctionWrapper<OP>::template Operation<INPUT_TYPE, RETURN_TYPE>(input);
	});
}

template <class OP, class INPUT_TYPE>
static ScalarFunction TemplatedGetHashFunction(LogicalType input_type, LogicalType return_type) {
	switch (return_type.InternalType()) {
	case PhysicalType::UINT32:
		return ScalarFunction({input_type}, return_type, HashFunction<OP, INPUT_TYPE, uint32_t>);
	case PhysicalType::UINT64:
		return ScalarFunction({input_type}, return_type, HashFunction<OP, INPUT_TYPE, uint64_t>);
	default:
		throw NotImplementedException("TODO");
	}
}

template <class OP>
static ScalarFunction GetHashFunction(LogicalType input_type, LogicalType return_type) {
	switch (input_type.InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::UINT8:
	case PhysicalType::INT8:
		return TemplatedGetHashFunction<OP, uint8_t>(input_type, return_type);
	case PhysicalType::UINT16:
	case PhysicalType::INT16:
		return TemplatedGetHashFunction<OP, uint16_t>(input_type, return_type);
	case PhysicalType::UINT32:
	case PhysicalType::INT32:
		return TemplatedGetHashFunction<OP, uint32_t>(input_type, return_type);
	case PhysicalType::UINT64:
	case PhysicalType::INT64:
		return TemplatedGetHashFunction<OP, uint64_t>(input_type, return_type);
	case PhysicalType::FLOAT:
		return TemplatedGetHashFunction<OP, float>(input_type, return_type);
	case PhysicalType::DOUBLE:
		return TemplatedGetHashFunction<OP, double>(input_type, return_type);
	case PhysicalType::INTERVAL:
		return TemplatedGetHashFunction<OP, interval_t>(input_type, return_type);
	case PhysicalType::INT128:
		return TemplatedGetHashFunction<OP, hugeint_t>(input_type, return_type);
	case PhysicalType::VARCHAR:
		return TemplatedGetHashFunction<OP, string_t>(input_type, return_type);
	default:
		throw NotImplementedException("GetTemplatedHashFunction for '%s'", TypeIdToString(input_type.InternalType()));
	}
}

template <class OP>
static void AddHashFunction(ClientContext &context, Catalog &catalog, const vector<LogicalType> &input_types,
                            const string &name, LogicalType return_type) {
	ScalarFunctionSet set(name);
	for (auto &type : input_types) {
		set.AddFunction(GetHashFunction<OP>(type, return_type));
	}
	CreateScalarFunctionInfo info(set);
	catalog.AddFunction(context, &info);
}

void HashExtension::Load(DuckDB &db) {
	static const vector<LogicalType> HASHABLE_TYPES = {
	    LogicalTypeId::BOOLEAN,      LogicalTypeId::TINYINT,       LogicalTypeId::UTINYINT,
	    LogicalTypeId::SMALLINT,     LogicalTypeId::USMALLINT,     LogicalTypeId::INTEGER,
	    LogicalTypeId::UINTEGER,     LogicalTypeId::BIGINT,        LogicalTypeId::UBIGINT,
	    LogicalTypeId::FLOAT,        LogicalTypeId::DOUBLE,        LogicalTypeId::DATE,
	    LogicalTypeId::TIMESTAMP,    LogicalTypeId::TIMESTAMP_SEC, LogicalTypeId::TIMESTAMP_MS,
	    LogicalTypeId::TIMESTAMP_NS, LogicalTypeId::TIME,          LogicalTypeId::TIMESTAMP_TZ,
	    LogicalTypeId::TIME_TZ,      LogicalTypeId::VARCHAR,       LogicalTypeId::BLOB,
	    LogicalTypeId::INTERVAL,     LogicalTypeId::HUGEINT,       LogicalTypeId::UUID,
	    LogicalTypeId::UBIGINT,      LogicalTypeId::JSON,          LogicalTypeId::BIGINT};

	Connection con(db);
	con.BeginTransaction();

	auto &context = *con.context;
	auto &catalog = Catalog::GetCatalog(context);

	AddHashFunction<XXHash>(context, catalog, HASHABLE_TYPES, "xxHash32", LogicalType::UINTEGER);
	AddHashFunction<XXHash>(context, catalog, HASHABLE_TYPES, "xxHash64", LogicalType::UBIGINT);

	con.Commit();
}

std::string HashExtension::Name() {
	return "hash";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void hash_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::HashExtension>();
}

DUCKDB_EXTENSION_API const char *hash_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
