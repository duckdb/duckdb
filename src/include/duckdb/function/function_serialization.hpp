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
private:
	static QualifiedName DeserializeQualifiedName(Deserializer &deserializer, Identifier name) {
		auto catalog = deserializer.ReadPropertyWithDefault<Identifier>(505, "catalog_name");
		auto schema = deserializer.ReadPropertyWithDefault<Identifier>(506, "schema_name");
		auto qname = deserializer.ReadPropertyWithExplicitDefault<QualifiedName>(507, "qname", QualifiedName());
		if (!qname.Name().empty()) {
			if (qname.Schema().empty()) {
				return QualifiedName(qname.Catalog().empty() ? Identifier::SystemCatalog() : qname.Catalog(),
				                     Identifier::DefaultSchema(), qname.Name());
			}
			return qname.Catalog().empty() ? qname.WithCatalog(Identifier::SystemCatalog()) : qname;
		}
		return QualifiedName(catalog.empty() ? Identifier::SystemCatalog() : catalog,
		                     schema.empty() ? Identifier::DefaultSchema() : schema, std::move(name));
	}

	class DeserializeContext {
	public:
		DeserializeContext(Deserializer &deserializer_p, const LogicalType &return_type,
		                   const const_expression_list_t &children)
		    : deserializer(deserializer_p) {
			deserializer.Set<const LogicalType &>(return_type);
			try {
				deserializer.Set<const const_expression_list_t &>(children);
			} catch (...) {
				deserializer.Unset<LogicalType>();
				throw;
			}
		}
		~DeserializeContext() { // NOLINT(bugprone-exception-escape): Unset only throws if the stack invariant is
			                    // broken.
			deserializer.Unset<const_expression_list_t>();
			deserializer.Unset<LogicalType>();
		}
		DeserializeContext(const DeserializeContext &) = delete;
		DeserializeContext &operator=(const DeserializeContext &) = delete;

	private:
		Deserializer &deserializer;
	};

	template <class FUNC>
	static void RestoreLogicalSignature(FUNC &function, const vector<unique_ptr<Expression>> &children,
	                                    const LogicalType &return_type) {
		auto arguments = function.GetArguments();
		for (idx_t index = 0; index < arguments.size() && index < children.size(); index++) {
			if (!arguments[index].IsComplete()) {
				arguments[index] = children[index]->GetReturnType();
			}
		}
		function.SetLogicalArguments(std::move(arguments));
		function.SetLogicalReturnType(return_type.IsAggregateState() ? function.GetReturnType() : return_type);
	}

public:
	template <class FUNC>
	static void Serialize(Serializer &serializer, const FUNC &function, optional_ptr<FunctionData> bind_info) {
		D_ASSERT(!function.GetName().empty());
		if (!serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
			serializer.WriteProperty(500, "name", function.GetName());
		}
		serializer.WriteProperty(501, "arguments", function.GetArguments());
		if (!serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
			// binds no longer erase the arguments they fold into their bind data, so the argument list above is
			// always the full one - older versions read this field unconditionally, so write it (empty) for them
			serializer.WriteProperty(502, "original_arguments", vector<LogicalType>());
		}

		if (serializer.ShouldSerialize(StorageVersion::V2_0_0)) {
			serializer.WriteProperty(507, "qname", function.GetQualifiedName());
		} else {
			serializer.WritePropertyWithDefault(505, "catalog_name", function.GetCatalogName(), Identifier());
			serializer.WritePropertyWithDefault(506, "schema_name", function.GetSchemaName(), Identifier());
		}

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
	static FUNC DeserializeFunction(ClientContext &context, CatalogType catalog_type,
	                                const QualifiedName &qualified_name, const vector<LogicalType> &arguments) {
		auto &func_catalog = Catalog::GetEntry(context, catalog_type, qualified_name);

		if (func_catalog.type != catalog_type) {
			throw InternalException("DeserializeFunction - cant find catalog entry for function %s",
			                        qualified_name.Name().GetIdentifierName());
		}
		auto &functions = func_catalog.Cast<CATALOG_ENTRY>();
		return *functions.functions.GetFunctionByArguments(context, arguments);
	}

	template <class FUNC, class CATALOG_ENTRY>
	static pair<FUNC, bool> DeserializeBase(Deserializer &deserializer, CatalogType catalog_type,
	                                        optional_ptr<vector<unique_ptr<Expression>>> children = nullptr) {
		auto &context = deserializer.Get<ClientContext &>();
		auto name = deserializer.ReadPropertyWithDefault<Identifier>(500, "name");
		auto arguments = deserializer.ReadProperty<vector<LogicalType>>(501, "arguments");
		auto original_arguments = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(502, "original_arguments");
		auto qualified_name = DeserializeQualifiedName(deserializer, std::move(name));
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

		auto function = DeserializeFunction<FUNC, CATALOG_ENTRY>(context, catalog_type, qualified_name, arguments);
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

		auto name = deserializer.ReadPropertyWithDefault<Identifier>(500, "name");
		auto arguments = deserializer.ReadProperty<vector<LogicalType>>(501, "arguments");
		auto original_arguments = deserializer.ReadPropertyWithDefault<vector<LogicalType>>(502, "original_arguments");
		auto qualified_name = DeserializeQualifiedName(deserializer, std::move(name));
		auto has_serialize = deserializer.ReadProperty<bool>(503, "has_serialize");

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
		auto &func_catalog = Catalog::GetEntry(context, catalog_type, qualified_name);

		if (func_catalog.type != catalog_type) {
			throw InternalException("DeserializeFunction - cant find catalog entry for function %s",
			                        qualified_name.Name().GetIdentifierName());
		}
		auto &functions = func_catalog.Cast<CATALOG_ENTRY>();
		const auto function = functions.functions.GetFunctionByArguments(context, arguments);

		// Does this function support serializing its bound data?
		if (!has_serialize) {
			// No, then just rebind the function
			try {
				FunctionBinder binder(context);

				auto [bound_function, bound_data] = binder.ResolveFunction(function, children);

				if (TypeRequiresAssignment(bound_function.GetReturnType())) {
					bound_function.SetReturnType(std::move(return_type));
				}

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
		RestoreLogicalSignature(bound_function, children, return_type);

		// Invoke deserialization function
		const_expression_list_t child_references;
		for (auto &child : children) {
			child_references.emplace_back(*child);
		}
		unique_ptr<FunctionData> bound_data;
		{
			DeserializeContext scope(deserializer, return_type, child_references);
			bound_data = FunctionDeserialize(deserializer, bound_function);
		}

		if (TypeRequiresAssignment(bound_function.GetReturnType())) {
			bound_function.SetReturnType(std::move(return_type));
		}

		return make_pair(std::move(bound_function), std::move(bound_data));
	}
};

} // namespace duckdb
