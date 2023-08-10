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
#include "duckdb/common/serializer/format_serializer.hpp"
#include "duckdb/common/serializer/format_deserializer.hpp"

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
			function.serialize(writer, bind_info, function);
			// First check if serialize throws a NotImplementedException, in which case it doesn't require a deserialize
			// function
			D_ASSERT(function.deserialize);
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

		auto &func_catalog = Catalog::GetEntry(context, type, SYSTEM_CATALOG, DEFAULT_SCHEMA, name);
		if (func_catalog.type != type) {
			throw InternalException("Cant find catalog entry for function %s", name);
		}

		auto &functions = func_catalog.Cast<CATALOG_ENTRY>();
		auto function = functions.functions.GetFunctionByArguments(
		    state.context, original_arguments.empty() ? arguments : original_arguments);
		function.arguments = std::move(arguments);
		function.original_arguments = std::move(original_arguments);

		has_deserialize = reader.ReadRequired<bool>();
		if (has_deserialize) {
			if (!function.deserialize) {
				throw SerializationException("Function requires deserialization but no deserialization function for %s",
				                             function.name);
			}
			bind_info = function.deserialize(state, reader, function);
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

	template <class FUNC>
	static void FormatSerialize(FormatSerializer &serializer, const FUNC &function,
	                            optional_ptr<FunctionData> bind_info) {
		D_ASSERT(!function.name.empty());
		serializer.WriteProperty(500, "name", function.name);
		serializer.WriteProperty(501, "arguments", function.arguments);
		serializer.WriteProperty(502, "original_arguments", function.original_arguments);
		bool has_serialize = function.format_serialize;
		serializer.WriteProperty(503, "has_serialize", has_serialize);
		if (has_serialize) {
			serializer.BeginObject(504, "function_data");
			function.format_serialize(serializer, bind_info, function);
			serializer.EndObject();
			D_ASSERT(function.format_deserialize);
		}
	}

	template <class FUNC, class CATALOG_ENTRY>
	static FUNC DeserializeFunction(ClientContext &context, CatalogType catalog_type, const string &name,
	                                vector<LogicalType> arguments, vector<LogicalType> original_arguments) {
		auto &func_catalog = Catalog::GetEntry(context, catalog_type, SYSTEM_CATALOG, DEFAULT_SCHEMA, name);
		if (func_catalog.type != catalog_type) {
			throw InternalException("DeserializeFunction - cant find catalog entry for function %s", name);
		}
		auto &functions = func_catalog.Cast<CATALOG_ENTRY>();
		auto function = functions.functions.GetFunctionByArguments(
		    context, original_arguments.empty() ? arguments : original_arguments);
		function.arguments = std::move(arguments);
		function.original_arguments = std::move(original_arguments);
		return function;
	}

	template <class FUNC, class CATALOG_ENTRY>
	static pair<FUNC, bool> FormatDeserializeBase(FormatDeserializer &deserializer, CatalogType catalog_type) {
		auto &context = deserializer.Get<ClientContext &>();
		auto name = deserializer.ReadProperty<string>(500, "name");
		auto arguments = deserializer.ReadProperty<vector<LogicalType>>(501, "arguments");
		auto original_arguments = deserializer.ReadProperty<vector<LogicalType>>(502, "original_arguments");
		auto function = DeserializeFunction<FUNC, CATALOG_ENTRY>(context, catalog_type, name, std::move(arguments),
		                                                         std::move(original_arguments));
		auto has_serialize = deserializer.ReadProperty<bool>(503, "has_serialize");
		return make_pair(std::move(function), has_serialize);
	}

	template <class FUNC>
	static unique_ptr<FunctionData> FunctionDeserialize(FormatDeserializer &deserializer, FUNC &function) {
		if (!function.format_deserialize) {
			throw SerializationException("Function requires deserialization but no deserialization function for %s",
			                             function.name);
		}
		deserializer.BeginObject(504, "function_data");
		auto result = function.format_deserialize(deserializer, function);
		deserializer.EndObject();
		return result;
	}

	template <class FUNC, class CATALOG_ENTRY>
	static pair<FUNC, unique_ptr<FunctionData>> FormatDeserialize(FormatDeserializer &deserializer,
	                                                              CatalogType catalog_type,
	                                                              vector<unique_ptr<Expression>> &children) {
		auto &context = deserializer.Get<ClientContext &>();
		auto entry = FormatDeserializeBase<FUNC, CATALOG_ENTRY>(deserializer, catalog_type);
		auto &function = entry.first;
		auto has_serialize = entry.second;

		unique_ptr<FunctionData> bind_data;
		if (has_serialize) {
			bind_data = FunctionDeserialize<FUNC>(deserializer, function);
		} else if (function.bind) {
			try {
				bind_data = function.bind(context, function, children);
			} catch (Exception &ex) {
				// FIXME
				throw SerializationException("Error during bind of function in deserialization: %s", ex.what());
			}
		}
		return make_pair(std::move(function), std::move(bind_data));
	}
};

} // namespace duckdb
