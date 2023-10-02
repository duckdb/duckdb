//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_serialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

namespace duckdb {

class FunctionSerializer {
public:
	template <class FUNC>
	static void Serialize(Serializer &serializer, const FUNC &function, optional_ptr<FunctionData> bind_info) {
		D_ASSERT(!function.name.empty());
		serializer.WriteProperty(500, "name", function.name);
		serializer.WriteProperty(501, "arguments", function.arguments);
		serializer.WriteProperty(502, "original_arguments", function.original_arguments);
		bool has_serialize = function.serialize;
		serializer.WriteProperty(503, "has_serialize", has_serialize);
		if (has_serialize) {
			serializer.WriteObject(504, "function_data",
			                       [&](Serializer &obj) { function.serialize(obj, bind_info, function); });
			D_ASSERT(function.deserialize);
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
	static pair<FUNC, bool> DeserializeBase(Deserializer &deserializer, CatalogType catalog_type) {
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
	static unique_ptr<FunctionData> FunctionDeserialize(Deserializer &deserializer, FUNC &function) {
		if (!function.deserialize) {
			throw SerializationException("Function requires deserialization but no deserialization function for %s",
			                             function.name);
		}
		unique_ptr<FunctionData> result;
		deserializer.ReadObject(504, "function_data",
		                        [&](Deserializer &obj) { result = function.deserialize(obj, function); });
		return result;
	}

	template <class FUNC, class CATALOG_ENTRY>
	static pair<FUNC, unique_ptr<FunctionData>> Deserialize(Deserializer &deserializer, CatalogType catalog_type,
	                                                        vector<unique_ptr<Expression>> &children,
	                                                        LogicalType return_type) {
		auto &context = deserializer.Get<ClientContext &>();
		auto entry = DeserializeBase<FUNC, CATALOG_ENTRY>(deserializer, catalog_type);
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
		function.return_type = std::move(return_type);
		return make_pair(std::move(function), std::move(bind_data));
	}
};

} // namespace duckdb
