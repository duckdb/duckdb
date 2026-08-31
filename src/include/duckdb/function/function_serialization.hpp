//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_serialization.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/function/function_binder.hpp"

namespace duckdb {
class ClientContext;

class FunctionSerializer {
public:
	template <class FUNC>
	static void Serialize(Serializer &serializer, const FUNC &function, optional_ptr<FunctionData> bind_info) {
		D_ASSERT(!function.GetName().empty());
		serializer.WriteProperty(500, "name", function.GetName());
		serializer.WriteProperty(501, "arguments", function.GetArguments());
		if (!serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
			// binds no longer erase the arguments they fold into their bind data, so the argument list above is
			// always the full one - older versions read this field unconditionally, so write it (empty) for them
			serializer.WriteProperty(502, "original_arguments", vector<LogicalType>());
		}
		// These are optional fields that are written out of numeric order, older
		// databases won't contain the fields, so the defaults will be used, but if
		// the fields are present, they will be used.
		serializer.WritePropertyWithDefault<Identifier>(505, "catalog_name", function.GetCatalogName(), Identifier());
		serializer.WritePropertyWithDefault<Identifier>(506, "schema_name", function.GetSchemaName(), Identifier());
		SerializeSQLDefinition(serializer, function);

		bool has_serialize = function.HasSerializationCallbacks();
		serializer.WriteProperty(503, "has_serialize", has_serialize);
		if (has_serialize) {
			serializer.WriteObject(504, "function_data",
			                       [&](Serializer &obj) { function.GetSerializeCallback()(obj, bind_info, function); });
			D_ASSERT(function.GetDeserializeCallback());
		}
	}

	//! Plans written by versions whose binds erased the arguments they folded into their bind data record the
	//! pre-erase list separately - use that as the argument list, so that the function looks the same either way
	static void RestoreErasedArguments(vector<LogicalType> &arguments, vector<LogicalType> &original_arguments) {
		if (!original_arguments.empty()) {
			arguments = std::move(original_arguments);
		}
	}

	template <class FUNC, class CATALOG_ENTRY>
	static FUNC DeserializeFunction(ClientContext &context, CatalogType catalog_type, const Identifier &catalog_name,
	                                const Identifier &schema_name, const Identifier &name,
	                                const vector<LogicalType> &arguments) {
		return *LookupFunction<CATALOG_ENTRY>(context, catalog_type, catalog_name, schema_name, name, arguments);
	}

	template <class FUNC, class CATALOG_ENTRY>
	static pair<FUNC, bool> DeserializeBase(Deserializer &deserializer, CatalogType catalog_type,
	                                        optional_ptr<vector<unique_ptr<Expression>>> children = nullptr) {
		auto &context = deserializer.Get<ClientContext &>();
		auto name = deserializer.ReadProperty<Identifier>(500, "name");
		auto arguments = deserializer.ReadProperty<vector<LogicalType>>(501, "arguments");
		auto original_arguments = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(502, "original_arguments");
		auto catalog_name = deserializer.ReadPropertyWithDefault<Identifier>(505, "catalog_name");
		auto schema_name = deserializer.ReadPropertyWithDefault<Identifier>(506, "schema_name");
		if (catalog_name.empty()) {
			catalog_name = Identifier::SystemCatalog();
		}
		if (schema_name.empty()) {
			schema_name = Identifier::DefaultSchema();
		}
		RestoreErasedArguments(arguments, original_arguments);

		if (arguments.empty() && children && !children->empty()) {
			// The function is specified as having no arguments, but somehow expressions were passed anyway
			// Assume this is a "varargs" function and use the types of the expressions as the arguments
			// This can happen when we change a function that used to take varargs, to no longer do so.
			arguments.reserve(children->size());
			for (auto &child : *children) {
				arguments.push_back(child->GetReturnType());
			}
		}

		auto function =
		    DeserializeFunction<FUNC, CATALOG_ENTRY>(context, catalog_type, catalog_name, schema_name, name, arguments);
		auto has_serialize = deserializer.ReadProperty<bool>(503, "has_serialize");
		if (has_serialize) {
			function.GetArguments() = std::move(arguments);
		}
		return make_pair(std::move(function), has_serialize);
	}

	template <class FUNC>
	static unique_ptr<FunctionData> FunctionDeserialize(Deserializer &deserializer, FUNC &function) {
		if (!function.HasSerializationCallbacks()) {
			throw SerializationException("Function requires deserialization but no deserialization function for %s",
			                             function.GetName());
		}
		unique_ptr<FunctionData> result;
		deserializer.ReadObject(504, "function_data",
		                        [&](Deserializer &obj) { result = function.GetDeserializeCallback()(obj, function); });
		return result;
	}

	static bool TypeRequiresAssignment(const LogicalType &type) {
		switch (type.id()) {
		case LogicalTypeId::SQLNULL:
		case LogicalTypeId::ANY:
		case LogicalTypeId::INVALID:
			return true;
		case LogicalTypeId::DECIMAL:
		case LogicalTypeId::UNION:
		case LogicalTypeId::VARIANT:
		case LogicalTypeId::MAP:
			if (!type.AuxInfo()) {
				return true;
			}
			return false;
		case LogicalTypeId::LIST:
			if (!type.AuxInfo()) {
				return true;
			}
			return TypeRequiresAssignment(ListType::GetChildType(type));
		case LogicalTypeId::ARRAY:
			if (!type.AuxInfo()) {
				return true;
			}
			return TypeRequiresAssignment(ArrayType::GetChildType(type));
		case LogicalTypeId::STRUCT:
		case LogicalTypeId::TUPLE:
			if (!type.AuxInfo()) {
				return true;
			}
			if (StructType::GetChildCount(type) == 0) {
				return true;
			}
			return false;
		default:
			return false;
		}
	}

	template <class FUNC, class CATALOG_ENTRY>
	static pair<FUNC, unique_ptr<FunctionData>> Deserialize(Deserializer &deserializer, CatalogType catalog_type,
	                                                        vector<unique_ptr<Expression>> &children,
	                                                        LogicalType return_type) { // NOLINT: clang-tidy bug
		auto &context = deserializer.Get<ClientContext &>();

		auto name = deserializer.ReadProperty<Identifier>(500, "name");
		auto arguments = deserializer.ReadProperty<vector<LogicalType>>(501, "arguments");
		auto original_arguments = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(502, "original_arguments");
		auto catalog_name = deserializer.ReadPropertyWithDefault<Identifier>(505, "catalog_name");
		auto schema_name = deserializer.ReadPropertyWithDefault<Identifier>(506, "schema_name");
		auto has_sql_definition = deserializer.ReadPropertyWithDefault<bool>(507, "has_sql_definition");
		Identifier sql_definition_name;
		Identifier sql_definition_catalog;
		Identifier sql_definition_schema;
		if (has_sql_definition) {
			sql_definition_name = deserializer.ReadProperty<Identifier>(508, "sql_definition_name");
			sql_definition_catalog = deserializer.ReadProperty<Identifier>(509, "sql_definition_catalog");
			sql_definition_schema = deserializer.ReadProperty<Identifier>(510, "sql_definition_schema");
		}
		auto has_serialize = deserializer.ReadProperty<bool>(503, "has_serialize");

		if (catalog_name.empty()) {
			catalog_name = Identifier::SystemCatalog();
		}
		if (schema_name.empty()) {
			schema_name = Identifier::DefaultSchema();
		}
		RestoreErasedArguments(arguments, original_arguments);

		if (arguments.empty() && !children.empty()) {
			// The function is specified as having no arguments, but somehow expressions were passed anyway
			// Assume this is a "varargs" function and use the types of the expressions as the arguments
			// This can happen when we change a function that used to take varargs, to no longer do so.
			arguments.reserve(children.size());
			for (auto &child : children) {
				arguments.push_back(child->GetReturnType());
			}
		}

		// Now lookup the function in the catalog.
		EntryLookupInfo lookup_info(catalog_type, QualifiedName(name));
		auto &func_catalog = Catalog::GetEntry(context, catalog_type, QualifiedName(catalog_name, schema_name, name));

		if (func_catalog.type != catalog_type) {
			throw InternalException("DeserializeFunction - cant find catalog entry for function %s",
			                        name.GetIdentifierName());
		}
		auto &functions = func_catalog.Cast<CATALOG_ENTRY>();
		auto function = functions.functions.GetFunctionByArguments(context, arguments);
		auto sql_definition = decltype(function)();
		if (has_sql_definition) {
			sql_definition = LookupFunction<CATALOG_ENTRY>(context, catalog_type, sql_definition_catalog,
			                                               sql_definition_schema, sql_definition_name, arguments);
		}

		// Does this function support serializing its bound data?
		if (!has_serialize) {
			// No, then just rebind the function
			try {
				FunctionBinder binder(context);

				auto [bound_function, bound_data] = binder.ResolveFunction(function, children);
				const FUNC &const_bound_function = bound_function;

				if (TypeRequiresAssignment(const_bound_function.GetReturnType())) {
					bound_function.SetReturnType(std::move(return_type));
				}
				RestoreFunctionIdentity(bound_function, std::move(sql_definition));

				return make_pair(std::move(bound_function), std::move(bound_data));
			} catch (std::exception &ex) {
				ErrorData error(ex);
				throw SerializationException("Error during bind of function in deserialization: %s",
				                             error.RawMessage());
			}
		}

		// Otherwise, construct the bound function from its parts
		FUNC bound_function(function);
		bound_function.GetArguments() = std::move(arguments);
		RestoreFunctionIdentity(bound_function, sql_definition);
		auto definition = GetFunctionDefinition(bound_function);
		auto preserves_function_identity = DeserializationPreservesFunctionIdentity(bound_function);
		auto restore_rebindable_definition = preserves_function_identity && HasRebindableDefinition(bound_function);
		ConsumeFunctionIdentity(bound_function);

		// Invoke deserialization function
		deserializer.Set<const LogicalType &>(return_type);
		auto bound_data = FunctionDeserialize(deserializer, bound_function);
		deserializer.Unset<LogicalType>();

		const FUNC &const_bound_function = bound_function;
		auto definition_unchanged = GetFunctionDefinition(const_bound_function) == definition;
		if (TypeRequiresAssignment(const_bound_function.GetReturnType())) {
			bound_function.SetReturnType(std::move(return_type));
		}
		RestoreFunctionIdentityAfterDeserialization(bound_function, preserves_function_identity && definition_unchanged,
		                                            restore_rebindable_definition && definition_unchanged);

		return make_pair(std::move(bound_function), std::move(bound_data));
	}

private:
	template <class CATALOG_ENTRY>
	static auto LookupFunction(ClientContext &context, CatalogType catalog_type, const Identifier &catalog_name,
	                           const Identifier &schema_name, const Identifier &name,
	                           const vector<LogicalType> &arguments) {
		EntryLookupInfo lookup_info(catalog_type, QualifiedName(name));
		auto &func_catalog =
		    Catalog::GetEntry(context, catalog_type,
		                      QualifiedName(catalog_name.empty() ? Identifier::SystemCatalog() : catalog_name,
		                                    schema_name.empty() ? Identifier::DefaultSchema() : schema_name, name));

		if (func_catalog.type != catalog_type) {
			throw InternalException("DeserializeFunction - cant find catalog entry for function %s",
			                        name.GetIdentifierName());
		}
		auto &functions = func_catalog.Cast<CATALOG_ENTRY>();
		return functions.functions.GetFunctionByArguments(context, arguments);
	}

	static void SerializeSQLDefinition(Serializer &serializer, const BoundScalarFunction &function) {
		SerializeSQLDefinition(serializer, function.GetDefinition(), function.HasRebindableDefinition());
	}
	static void SerializeSQLDefinition(Serializer &serializer, const BoundAggregateFunction &function) {
		SerializeSQLDefinition(serializer, function.GetDefinition(), function.HasRebindableDefinition());
	}
	template <class FUNC>
	static void SerializeSQLDefinition(Serializer &, const FUNC &) {
	}
	template <class FUNC>
	static void SerializeSQLDefinition(Serializer &serializer, const shared_ptr<const FUNC> &definition,
	                                   bool rebindable) {
		serializer.WritePropertyWithDefault<bool>(507, "has_sql_definition", rebindable, false);
		if (!rebindable) {
			return;
		}
		D_ASSERT(definition);
		serializer.WriteProperty(508, "sql_definition_name", definition->GetName());
		serializer.WriteProperty(509, "sql_definition_catalog", definition->GetCatalogName());
		serializer.WriteProperty(510, "sql_definition_schema", definition->GetSchemaName());
	}

	static void RestoreFunctionIdentity(BoundScalarFunction &function,
	                                    shared_ptr<const ScalarFunction> sql_definition) {
		if (sql_definition) {
			function.SetDefinition(std::move(sql_definition));
			function.RestoreFunctionExpressionIdentity();
			function.RestoreRebindableDefinition();
			return;
		}
		function.InvalidateRebindableDefinition();
		function.RestoreFunctionExpressionIdentity();
	}
	static void RestoreFunctionIdentity(BoundAggregateFunction &function,
	                                    shared_ptr<const AggregateFunction> sql_definition) {
		if (sql_definition) {
			function.SetDefinition(std::move(sql_definition));
			function.RestoreRebindableDefinition();
			return;
		}
		function.InvalidateRebindableDefinition();
	}
	static shared_ptr<const ScalarFunction> GetFunctionDefinition(const BoundScalarFunction &function) {
		return function.GetDefinition();
	}
	static shared_ptr<const AggregateFunction> GetFunctionDefinition(const BoundAggregateFunction &function) {
		return function.GetDefinition();
	}
	static bool HasRebindableDefinition(const BoundScalarFunction &function) {
		return function.HasRebindableDefinition();
	}
	static bool HasRebindableDefinition(const BoundAggregateFunction &function) {
		return function.HasRebindableDefinition();
	}
	static bool DeserializationPreservesFunctionIdentity(const BoundScalarFunction &function) {
		return function.DeserializationPreservesFunctionIdentity();
	}
	static bool DeserializationPreservesFunctionIdentity(const BoundAggregateFunction &function) {
		return function.DeserializationPreservesFunctionIdentity();
	}
	static void ConsumeFunctionIdentity(BoundScalarFunction &function) {
		function.InvalidateFunctionExpressionIdentity();
	}
	static void ConsumeFunctionIdentity(BoundAggregateFunction &function) {
		function.InvalidateRebindableDefinition();
	}
	static void RestoreFunctionIdentityAfterDeserialization(BoundScalarFunction &function,
	                                                        bool restore_expression_identity,
	                                                        bool restore_rebindable_definition) {
		if (restore_expression_identity) {
			function.RestoreFunctionExpressionIdentity();
		}
		if (restore_rebindable_definition) {
			function.RestoreRebindableDefinition();
		}
	}
	static void RestoreFunctionIdentityAfterDeserialization(BoundAggregateFunction &function, bool,
	                                                        bool restore_rebindable_definition) {
		if (restore_rebindable_definition) {
			function.RestoreRebindableDefinition();
		}
	}
	template <class FUNC, class DEFINITION>
	static void RestoreFunctionIdentity(FUNC &, DEFINITION) {
	}
	template <class FUNC>
	static bool HasRebindableDefinition(const FUNC &) {
		return false;
	}
	template <class FUNC>
	static bool DeserializationPreservesFunctionIdentity(const FUNC &) {
		return false;
	}
	template <class FUNC>
	static nullptr_t GetFunctionDefinition(const FUNC &) {
		return nullptr;
	}
	template <class FUNC>
	static void ConsumeFunctionIdentity(FUNC &) {
	}
	template <class FUNC>
	static void RestoreFunctionIdentityAfterDeserialization(FUNC &, bool, bool) {
	}
};

} // namespace duckdb
