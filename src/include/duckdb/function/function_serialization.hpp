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
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"

namespace duckdb {

class FunctionSerializer {
public:
	template <class FUNC>
	static void SerializeBase(FieldWriter &writer, const FUNC &function, FunctionData *bind_info) {
		D_ASSERT(!function.name.empty());
		writer.WriteString(function.name);
		writer.WriteRegularSerializableList(function.arguments);
		writer.WriteRegularSerializableList(function.original_arguments);
		bool serialize = function.serialize;
		writer.WriteField(serialize);
		if (serialize) {
			D_ASSERT(function.deserialize);
			function.serialize(writer, bind_info, function);
		}
	}

	template <class FUNC>
	static void Serialize(FieldWriter &writer, const FUNC &function, const LogicalType &return_type,
	                      const vector<unique_ptr<Expression>> &children, FunctionData *bind_info) {
		SerializeBase(writer, function, bind_info);
		writer.WriteSerializable(return_type);
		writer.WriteSerializableList(children);
	}

	template <class FUNC, class CATALOG_ENTRY>
	static FUNC DeserializeBaseInternal(FieldReader &reader, PlanDeserializationState &state, CatalogType type,
	                                    unique_ptr<FunctionData> &bind_info, bool &has_deserialize) {
		auto &context = state.context;
		auto name = reader.ReadRequired<string>();
		auto arguments = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
		// note: original_arguments are optional (can be list of size 0)
		auto original_arguments = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();

		auto func_catalog = Catalog::GetEntry(context, type, INVALID_CATALOG, DEFAULT_SCHEMA, name);
		if (!func_catalog || func_catalog->type != type) {
			throw InternalException("Cant find catalog entry for function %s", name);
		}

		auto functions = (CATALOG_ENTRY *)func_catalog;
		auto function = functions->functions.GetFunctionByArguments(
		    state.context, original_arguments.empty() ? arguments : original_arguments);
		function.arguments = std::move(arguments);
		function.original_arguments = std::move(original_arguments);

		has_deserialize = reader.ReadRequired<bool>();
		if (has_deserialize) {
			if (!function.deserialize) {
				throw SerializationException("Function requires deserialization but no deserialization function for %s",
				                             function.name);
			}
			bind_info = function.deserialize(context, reader, function);
		} else {
			D_ASSERT(!function.serialize);
			D_ASSERT(!function.deserialize);
		}
		return function;
	}
	template <class FUNC, class CATALOG_ENTRY>
	static FUNC DeserializeBase(FieldReader &reader, PlanDeserializationState &state, CatalogType type,
	                            unique_ptr<FunctionData> &bind_info) {
		bool has_deserialize;
		return DeserializeBaseInternal<FUNC, CATALOG_ENTRY>(reader, state, type, bind_info, has_deserialize);
	}

	template <class FUNC, class CATALOG_ENTRY>
	static FUNC Deserialize(FieldReader &reader, ExpressionDeserializationState &state, CatalogType type,
	                        vector<unique_ptr<Expression>> &children, unique_ptr<FunctionData> &bind_info) {
		bool has_deserialize;
		auto function =
		    DeserializeBaseInternal<FUNC, CATALOG_ENTRY>(reader, state.gstate, type, bind_info, has_deserialize);
		auto return_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
		children = reader.ReadRequiredSerializableList<Expression>(state.gstate);

		// we re-bind the function only if the function did not have an explicit deserialize method
		auto &context = state.gstate.context;
		if (!has_deserialize && function.bind) {
			bind_info = function.bind(context, function, children);
		}
		function.return_type = return_type;
		return function;
	}
};

} // namespace duckdb
