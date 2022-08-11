//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_serialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/field_writer.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

class FunctionSerializer {
public:
	template <class FUNC>
	static void Serialize(FieldWriter &writer, const FUNC &function, const LogicalType &return_type,
	                       const vector<unique_ptr<Expression>> &children, FunctionData *bind_info) {
		D_ASSERT(!function.name.empty());
		writer.WriteString(function.name);
		writer.WriteSerializable(return_type);
		writer.WriteRegularSerializableList(function.arguments);
		writer.WriteSerializableList(children);

		bool serialize = function.serialize;
		writer.WriteField(serialize);
		if (serialize) {
			function.serialize(writer, bind_info, function);
		}
	}

	template <class FUNC, class CATALOG_ENTRY>
	static FUNC Deserialize(FieldReader &reader, ExpressionDeserializationState &state, CatalogType type,
	                         vector<unique_ptr<Expression>> &children, unique_ptr<FunctionData> &bind_info) {
		auto &context = state.gstate.context;
		auto name = reader.ReadRequired<string>();
		auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		auto arguments = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
		children = reader.ReadRequiredSerializableList<Expression>(state.gstate);

		// TODO this is duplicated in logical_get more or less, make it a template or so
		auto &catalog = Catalog::GetCatalog(context);
		auto func_catalog = catalog.GetEntry(context, type, DEFAULT_SCHEMA, name);
		if (!func_catalog || func_catalog->type != type) {
			throw InternalException("Cant find catalog entry for function %s", name);
		}

		auto functions = (CATALOG_ENTRY *)func_catalog;
		auto function = functions->functions.GetFunctionByArguments(arguments);

		// sometimes the bind changes those, not sure if we should generically set those
		function.return_type = return_type;
		function.arguments = move(arguments);

		auto has_deserialize = reader.ReadRequired<bool>();
		if (has_deserialize) {
			if (!function.deserialize) {
				throw SerializationException("Function requires deserialization but no deserialization function for %s",
				                             function.name);
			}
			bind_info = function.deserialize(context, reader, function);
		} else if (function.bind) {
			bind_info = function.bind(context, function, children);
		}
		return function;
	}
};

} // namespace duckdb
