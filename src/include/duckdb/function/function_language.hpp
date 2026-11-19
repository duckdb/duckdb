//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/function_language.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/parser/qualified_name.hpp"

#include <functional>

namespace duckdb {

class ClientContext;
class ParsedExpression;
class ScalarFunctionCatalogEntry;
struct ScalarFunctionInfo;

//! One argument of a call to a language member, in parameter declaration order. Core binds an untyped argument on
//! its own only when it is read, so an argument the callback does not read costs nothing
struct LanguageCallArgument {
	//! The bound type: the declared type of a typed parameter, UNKNOWN when the argument is not bound on its own
	LogicalType type = LogicalType::UNKNOWN;
	//! The argument as SQL text (before the parameter cast), for diagnostics and lambda detection only. It does
	//! not round-trip in every case and must not be re-parsed: the substitution carries the real expression
	string sql;
	//! The argument is the parameter's default
	bool is_default = false;
	//! The argument folds to value
	bool is_constant = false;
	Value value;
	//! Why the argument does not bind on its own (e.g. a lambda or a bare table name), empty otherwise
	string bind_error;
	//! Properties of the bound argument. Arguments with an aggregate or a window call are not bound on their own.
	//! Aggregates are found by name: an aggregate behind a macro call makes the argument fail to bind instead
	bool references_columns = false;
	bool is_volatile = false;
	bool has_aggregate = false;
	bool has_window = false;
	bool has_subquery = false;
};

//! The input of a language's call_replace callback: a call of a member written in the language. Every input is a
//! plain value owned by core and valid for the duration of the callback.
//! The callback returns a replacement expression written in terms of the parameter names, like a macro body. Core
//! substitutes the arguments for the parameter references and binds the result in the caller's scope. Without a
//! replacement, the call expands to <language>('<source>', <arguments>...).
//! The replacement binds in the caller's scope, so its unqualified names resolve there like a macro body's: a language
//! that wants its relations isolated from call-site CTEs qualifies them. The callback itself observes call-site CTEs
//! only through the arguments it reads.
//! The callback runs on every bind of a call. A language that caches its output keys it on every input it reads:
//! the source and the member's signature, the names it emits for parameters, the search path, and each argument it
//! reads. The expected pattern compiles per (source, argument types, search path) and specializes on constant
//! argument values separately.
class ScalarFunctionCallReplaceInput {
public:
	ScalarFunctionCallReplaceInput(ClientContext &context, optional_ptr<ScalarFunctionInfo> function_info,
	                               const QualifiedName &function_name, idx_t member_index, const string &language,
	                               const string &source, const vector<Identifier> &parameter_names,
	                               const vector<LogicalType> &parameter_types, const LogicalType &return_type,
	                               idx_t argument_count,
	                               std::function<const LanguageCallArgument &(idx_t)> argument_reader,
	                               const vector<CatalogSearchEntry> &search_path);

	ClientContext &context;
	//! The extra info of the language function's selected overload
	optional_ptr<ScalarFunctionInfo> function_info;
	//! The called function, catalog-qualified
	const QualifiedName &function_name;
	//! The index of the called member in the function's overload set
	idx_t member_index;
	const string &language;
	const string &source;
	const vector<Identifier> &parameter_names;
	//! The declared parameter types, UNKNOWN for an untyped parameter
	const vector<LogicalType> &parameter_types;
	//! The declared RETURNS type, UNKNOWN when none is declared. Core casts the replacement's result to it
	const LogicalType &return_type;
	//! The effective search path at the call site, where the replacement binds
	const vector<CatalogSearchEntry> &search_path;

public:
	//! The number of arguments, one per parameter
	idx_t ArgumentCount() const;
	//! One argument. Reading an argument binds it on its own, once per call, which may bind the call-site CTEs it
	//! references. A read argument the replacement uses is bound again with the replacement, the cost model of a
	//! typed macro, whose member selection binds each argument too: nested reading calls grow exponentially with the
	//! nesting depth, so a callback reads only the arguments it depends on
	const LanguageCallArgument &GetArgument(idx_t argument_idx);
	//! Set the replacement as SQL text of one expression, parsed with the builtin parser options
	void SetReplacement(string sql);
	//! Set the replacement as a parsed expression
	void SetReplacement(unique_ptr<ParsedExpression> expression);
	//! Rename the parameters as the replacement references them, one name per parameter
	void SetParameterNames(vector<Identifier> names);
	//! A note core appends to a binder, conversion or catalog error raised while binding the replacement
	void SetErrorNote(string note);

	bool HasReplacement() const;
	//! The replacement as set by the callback, parsed if it was set as SQL text
	unique_ptr<ParsedExpression> TakeReplacement();
	const vector<Identifier> &GetReplacementParameterNames() const;
	const string &GetErrorNote() const;

private:
	idx_t argument_count;
	std::function<const LanguageCallArgument &(idx_t)> argument_reader;
	optional<string> replacement_sql;
	unique_ptr<ParsedExpression> replacement_expression;
	vector<Identifier> replacement_parameter_names;
	string error_note;
};

//! One member of a definition under CREATE-time validation: copies of its text fields and the stored types
struct LanguageMemberView {
	string language;
	string source;
	vector<Identifier> parameter_names;
	//! UNKNOWN for an untyped parameter
	vector<LogicalType> parameter_types;
	//! The default as SQL text, empty for a parameter without a default
	vector<string> parameter_defaults;
	//! UNKNOWN when no RETURNS type is declared
	LogicalType return_type = LogicalType::UNKNOWN;
};

//! The input of a language's create_validate callback. The member types are the stored types: their extension
//! metadata stays mutable behind the const accessor, but modifying them is undefined behavior. CREATE compares the
//! persisted signature of every member before and after the callback and rejects a change, but cannot undo it: the
//! types share their info with the catalog and with types the extension registered. Core copies the rewritten
//! sources and the published descriptions back after the callback
class ScalarFunctionCreateValidateInput {
public:
	ScalarFunctionCreateValidateInput(ClientContext &context, const QualifiedName &function_name,
	                                  const string &statement_text, vector<LanguageMemberView> members);

	ClientContext &context;
	//! The function being created, catalog-qualified
	const QualifiedName &function_name;
	//! The CREATE statement text the definition was parsed from, empty for a definition built programmatically
	const string &statement_text;
	const vector<LanguageMemberView> members;

public:
	//! Rewrite the stored source of a member
	void SetMemberSource(idx_t member_idx, string source);
	//! Publish documentation. A description must carry its member's parameter types, with ANY for untyped
	//! parameters, so it is released with the member when the member is replaced or dropped
	void AddDescription(FunctionDescription description);

	//! The rewritten source of a member, if any
	optional_ptr<const string> GetMemberSource(idx_t member_idx) const;
	vector<FunctionDescription> TakeDescriptions();

private:
	vector<optional<string>> member_sources;
	vector<FunctionDescription> descriptions;
};

//! The scalar function set implementing a language, looked up in the system catalog with autoload by name. Returns
//! nullptr when no function of that name exists, and throws when the function is not declared as a language
optional_ptr<ScalarFunctionCatalogEntry> TryGetLanguageFunction(ClientContext &context, const string &language);

} // namespace duckdb
