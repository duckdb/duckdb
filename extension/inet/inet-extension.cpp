#define DUCKDB_EXTENSION_MAIN

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/common/types/cast_helpers.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "inet-extension.hpp"

namespace duckdb {

struct IPAddress {
	hugeint_t address;
	int32_t mask;
};

static bool IPAddressError(string_t input, string *error_message, string error) {
	string e = "Failed to convert string \"" + input.GetString() + "\" to inet: " + error;
	HandleCastError::AssignError(e, error_message);
	return false;
}

static bool TryParseIPAddress(string_t input, IPAddress &result, string *error_message) {
	auto data = input.GetDataUnsafe();
	auto size = input.GetSize();
	idx_t c = 0;
	idx_t number_count = 0;
	int32_t address = 0;
parse_number:
	idx_t start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	if (start == c) {
		return IPAddressError(input, error_message, "Expected a number");
	}
	uint8_t number;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), number)) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 255");
	}
	address <<= 8;
	address += number;
	number_count++;
	result.address = address;
	if (number_count == 4) {
		goto parse_mask;
	} else {
		goto parse_dot;
	}
parse_dot:
	if (c == size || data[c] != '.') {
		return IPAddressError(input, error_message, "Expected a dot");
	}
	c++;
	goto parse_number;
parse_mask:
	if (c == size) {
		// no mask, default to 32
		result.mask = 32;
		return true;
	}
	if (data[c] != '/') {
		return IPAddressError(input, error_message, "Expected a slash");
	}
	c++;
	start = c;
	while (c < size && data[c] >= '0' && data[c] <= '9') {
		c++;
	}
	uint8_t mask;
	if (!TryCast::Operation<string_t, uint8_t>(string_t(data + start, c - start), mask)) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	}
	if (mask > 32) {
		return IPAddressError(input, error_message, "Expected a number between 0 and 32");
	}
	result.mask = mask;
	return true;
}

static bool VarcharToINETCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto &entries = StructVector::GetEntries(result);
	auto address_data = FlatVector::GetData<hugeint_t>(*entries[0]);
	auto mask_data = FlatVector::GetData<uint16_t>(*entries[1]);

	auto input = (string_t *)vdata.data;
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		IPAddress inet;
		if (!TryParseIPAddress(input[idx], inet, parameters.error_message)) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		address_data[i] = inet.address;
		mask_data[i] = inet.mask;
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

static string INETToVarchar(IPAddress inet) {
	string result;
	for (idx_t i = 0; i < 4; i++) {
		if (i > 0) {
			result += ".";
		}
		uint8_t byte = Hugeint::Cast<uint8_t>((inet.address >> (3 - i) * 8) & 0xFF);
		auto str = to_string(byte);
		result += str;
	}
	if (inet.mask != 32) {
		result += "/" + to_string(inet.mask);
	}
	return result;
}

static bool INETToVarcharCast(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;
	source.Flatten(count);

	auto &entries = StructVector::GetEntries(source);
	auto &validity = FlatVector::Validity(source);
	auto address_data = FlatVector::GetData<hugeint_t>(*entries[0]);
	auto mask_data = FlatVector::GetData<uint16_t>(*entries[1]);
	auto result_data = FlatVector::GetData<string_t>(result);

	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		if (!validity.RowIsValid(i)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		IPAddress inet;
		inet.address = address_data[i];
		inet.mask = mask_data[i];
		auto str = INETToVarchar(inet);
		result_data[i] = StringVector::AddString(result, str);
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return true;
}

static void INETHost(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &entries = StructVector::GetEntries(args.data[0]);

	UnaryExecutor::Execute<hugeint_t, string_t>(*entries[0], result, args.size(), [&](hugeint_t input) {
		IPAddress inet;
		inet.address = input;
		inet.mask = 32;

		auto str = INETToVarchar(inet);
		return StringVector::AddString(result, str);
	});
}

void INETExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);

	// add the "inet" type
	child_list_t<LogicalType> children;
	children.push_back(make_pair("address", LogicalType::HUGEINT));
	children.push_back(make_pair("mask", LogicalType::USMALLINT));
	auto inet_type = LogicalType::STRUCT(move(children));
	inet_type.SetAlias("inet");

	CreateTypeInfo info("inet", inet_type);
	info.temporary = true;
	info.internal = true;
	catalog.CreateType(*con.context, &info);

	auto host_fun = ScalarFunction("host", {inet_type}, LogicalType::VARCHAR, INETHost);
	CreateScalarFunctionInfo host_info(host_fun);
	catalog.CreateFunction(*con.context, &host_info);

	// add inet casts
	auto &config = DBConfig::GetConfig(*con.context);

	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(LogicalType::VARCHAR, inet_type, VarcharToINETCast, 100);
	casts.RegisterCastFunction(inet_type, LogicalType::VARCHAR, INETToVarcharCast);

	con.Commit();
}

std::string INETExtension::Name() {
	return "inet";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void inet_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::INETExtension>();
}

DUCKDB_EXTENSION_API const char *inet_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
