#include "duckdb.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/function_language.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"

using namespace duckdb;

// testlang: a deliberately trivial language that exercises the language function layer from SQL tests. A member's
// source selects what the call_replace callback returns:
//   <text>                the constant <text> (a parsed expression handed back from C++)
//   arg:<n>               the n-th parameter
//   sql:<expr>            <expr> verbatim as SQL text
//   names:<a,b,..>|<expr> <expr> over renamed parameters
//   note:<text>|<expr>    <expr>, with an error note
//   error:<text>          the callback raises
//   decline               no replacement: the call expands to testlang('decline', args...)
//   countdown             x + f(x - 1), and 0 once x folds to a constant <= 0
//   types, values, props, sqltext, path, name
//                         a string rendering the corresponding callback input
// At CREATE, a source "rewrite:<x>" is stored as <x>, "invalid" is rejected, and "describe" publishes a description
// rendering the validation inputs.
// testinterp declares itself a language without a callback: every call expands to testinterp('<source>', args...).

static string RenderArguments(ScalarFunctionCallReplaceInput &input, const std::function<string(idx_t)> &render) {
	vector<string> parts;
	for (idx_t i = 0; i < input.ArgumentCount(); i++) {
		parts.push_back(render(i));
	}
	return StringUtil::Join(parts, ",");
}

static string RenderProperties(const LanguageCallArgument &argument) {
	string result;
	result += argument.is_default ? "d" : "";
	result += argument.is_constant ? "k" : "";
	result += argument.references_columns ? "c" : "";
	result += argument.is_volatile ? "v" : "";
	result += argument.has_aggregate ? "a" : "";
	result += argument.has_window ? "w" : "";
	result += argument.has_subquery ? "s" : "";
	result += argument.bind_error.empty() ? "" : "e";
	return result.empty() ? "-" : result;
}

static void SetStringReplacement(ScalarFunctionCallReplaceInput &input, const string &text) {
	input.SetReplacement(ConstantExpression::FromValue(Value(text)));
}

//! Split "<head>|<expr>" at the first '|'
static bool SplitHead(const string &body, string &head, string &expr) {
	auto pos = body.find('|');
	if (pos == string::npos) {
		return false;
	}
	head = body.substr(0, pos);
	expr = body.substr(pos + 1);
	return true;
}

static void TestLangCallReplace(ScalarFunctionCallReplaceInput &input) {
	auto &source = input.source;
	string head;
	string body;
	if (StringUtil::StartsWith(source, "arg:")) {
		idx_t argument_idx;
		const auto index_text = source.substr(4);
		if (index_text.empty() || !TryCast::Operation<string_t, idx_t>(string_t(index_text), argument_idx)) {
			throw BinderException("testlang: malformed argument index \"%s\"", index_text);
		}
		if (argument_idx >= input.parameter_names.size()) {
			throw BinderException("testlang: argument index %llu is out of range", argument_idx);
		}
		input.SetReplacement(SQLIdentifier::ToString(input.parameter_names[argument_idx]));
		return;
	}
	if (StringUtil::StartsWith(source, "sql:")) {
		input.SetReplacement(source.substr(4));
		return;
	}
	if (StringUtil::StartsWith(source, "names:") && SplitHead(source.substr(6), head, body)) {
		vector<Identifier> names;
		for (auto &name : StringUtil::Split(head, ',')) {
			names.emplace_back(name);
		}
		input.SetParameterNames(std::move(names));
		input.SetReplacement(body);
		return;
	}
	if (StringUtil::StartsWith(source, "note:") && SplitHead(source.substr(5), head, body)) {
		input.SetErrorNote(head);
		input.SetReplacement(body);
		return;
	}
	if (StringUtil::StartsWith(source, "error:")) {
		throw BinderException("testlang: %s", source.substr(6));
	}
	if (StringUtil::StartsWith(source, "probe:")) {
		// read every argument input before returning <expr>
		for (idx_t i = 0; i < input.ArgumentCount(); i++) {
			input.GetArgument(i);
		}
		input.SetReplacement(source.substr(6));
		return;
	}
	if (source == "decline") {
		return;
	}
	if (source == "countdown") {
		// expands to x + f(x - 1) until the first argument folds to a constant <= 0
		auto &first = input.GetArgument(0);
		if (first.is_constant && !first.value.IsNull() && first.value.GetValue<int64_t>() <= 0) {
			input.SetReplacement("0");
			return;
		}
		auto parameter = SQLIdentifier::ToString(input.parameter_names[0]);
		input.SetReplacement(parameter + " + " + input.function_name.ToString() + "(" + parameter + " - 1)");
		return;
	}
	if (source == "types") {
		vector<string> parameter_types;
		for (auto &type : input.parameter_types) {
			parameter_types.push_back(type.ToString());
		}
		auto argument_types = RenderArguments(input, [&](idx_t i) { return input.GetArgument(i).type.ToString(); });
		SetStringReplacement(input, argument_types + "|" + StringUtil::Join(parameter_types, ",") + "|" +
		                                input.return_type.ToString());
		return;
	}
	if (source == "values") {
		SetStringReplacement(input, RenderArguments(input, [&](idx_t i) {
			                     auto &argument = input.GetArgument(i);
			                     return argument.is_constant ? argument.value.ToSQLString() : string("?");
		                     }));
		return;
	}
	if (source == "props") {
		SetStringReplacement(input,
		                     RenderArguments(input, [&](idx_t i) { return RenderProperties(input.GetArgument(i)); }));
		return;
	}
	if (source == "sqltext") {
		SetStringReplacement(input, RenderArguments(input, [&](idx_t i) { return input.GetArgument(i).sql; }));
		return;
	}
	if (source == "path") {
		SetStringReplacement(input, CatalogSearchEntry::ListToString(input.search_path));
		return;
	}
	if (source == "name") {
		vector<string> names;
		for (auto &name : input.parameter_names) {
			names.push_back(name.GetIdentifierName());
		}
		SetStringReplacement(input, input.function_name.ToString() + "#" + to_string(input.member_index) + "|" +
		                                input.language + "|" + StringUtil::Join(names, ","));
		return;
	}
	// a constant ignores its arguments, but an argument that does not bind still surfaces
	for (idx_t i = 0; i < input.ArgumentCount(); i++) {
		auto &argument = input.GetArgument(i);
		if (!argument.bind_error.empty()) {
			throw BinderException(argument.bind_error);
		}
	}
	SetStringReplacement(input, source);
}

static void TestLangCreateValidate(ScalarFunctionCreateValidateInput &input) {
	for (auto &member : input.members) {
		if (StringUtil::Contains(member.source, "invalid")) {
			throw BinderException("testlang: the source of %s may not contain the word \"invalid\"",
			                      input.function_name.Name());
		}
	}
	for (idx_t member_idx = 0; member_idx < input.members.size(); member_idx++) {
		auto &member = input.members[member_idx];
		auto source = member.source;
		if (StringUtil::StartsWith(source, "rewrite:")) {
			source = source.substr(8);
			input.SetMemberSource(member_idx, source);
		}
		// publish per-member documentation: function metadata must survive member mutations
		FunctionDescription description;
		for (idx_t i = 0; i < member.parameter_names.size(); i++) {
			description.parameter_names.push_back(member.parameter_names[i].GetIdentifierName());
			// an untyped parameter is documented as a wildcard, like built-in function docs
			auto &type = member.parameter_types[i];
			description.parameter_types.push_back(type.id() == LogicalTypeId::UNKNOWN ? LogicalType::ANY : type);
		}
		// a per-source description, so tests distinguish stale metadata from replaced metadata
		description.description = "testlang: " + source;
		if (source == "describe") {
			// render the validation inputs
			vector<string> names;
			vector<string> types;
			for (idx_t i = 0; i < member.parameter_names.size(); i++) {
				names.push_back(member.parameter_names[i].GetIdentifierName());
				types.push_back(member.parameter_types[i].ToString());
			}
			description.description = input.function_name.ToString() + "|" + member.language + "|" +
			                          StringUtil::Join(names, ",") + "|" + StringUtil::Join(types, ",") + "|" +
			                          StringUtil::Join(member.parameter_defaults, ",") + "|" +
			                          member.return_type.ToString() + "|" + input.statement_text;
		}
		input.AddDescription(std::move(description));
	}
}

static atomic<idx_t> interpreter_calls {0};

//! The direct form L('<source>', args...): the source followed by the arguments, NULL arguments rendered as NULL, so
//! the functions declare special NULL handling
static void InterpretFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto writer = FlatVector::Writer<string_t>(result, count);
	for (idx_t row = 0; row < count; row++) {
		auto source = args.data[0].GetValue(row);
		string text = source.IsNull() ? "NULL" : source.ToString();
		if (StringUtil::StartsWith(text, "counter")) {
			text += "#" + to_string(++interpreter_calls);
		}
		vector<string> parts;
		for (idx_t col = 1; col < args.ColumnCount(); col++) {
			parts.push_back(args.data[col].GetValue(row).ToString());
		}
		writer.WriteValue(StringVector::AddString(result, text + "(" + StringUtil::Join(parts, ", ") + ")"));
	}
}

//! The two overloads of testpair replace a call by the overload the argument types select
static void PairBigintCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement("'bigint overload'");
}

static void PairVarcharCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement("'varchar overload'");
}

static void PlangCallReplace(ScalarFunctionCallReplaceInput &input) {
	input.SetReplacement("'plang language: " + input.source + "'");
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(language_function_demo, loader) {
	ScalarFunction testlang_function("testlang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                 InterpretFunction, nullptr, nullptr, nullptr, LogicalType::ANY);
	testlang_function.SetCallReplaceCallback(TestLangCallReplace);
	testlang_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	ScalarFunctionSet testlang("testlang");
	testlang.AddFunction(std::move(testlang_function));
	testlang.DeclareLanguage();
	testlang.SetCreateValidateCallback(TestLangCreateValidate);
	loader.RegisterFunction(std::move(testlang));

	// a language without a callback: calls run through the interpreter function, which is volatile
	ScalarFunction testinterp_function("testinterp", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   InterpretFunction, nullptr, nullptr, nullptr, LogicalType::ANY);
	testinterp_function.SetVolatile();
	testinterp_function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	ScalarFunctionSet testinterp("testinterp");
	testinterp.AddFunction(std::move(testinterp_function));
	testinterp.DeclareLanguage();
	loader.RegisterFunction(std::move(testinterp));

	// a scalar function that is not a language
	ScalarFunction not_a_language("testlang_plain", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                              InterpretFunction);
	not_a_language.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(std::move(not_a_language));

	// a language with two overloads, selected by the member call's argument types
	ScalarFunctionSet testpair("testpair");
	ScalarFunction pair_bigint("testpair", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::BIGINT},
	                           LogicalType::VARCHAR, InterpretFunction);
	pair_bigint.SetCallReplaceCallback(PairBigintCallReplace);
	pair_bigint.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	testpair.AddFunction(std::move(pair_bigint));
	ScalarFunction pair_varchar("testpair", vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                            LogicalType::VARCHAR, InterpretFunction);
	pair_varchar.SetCallReplaceCallback(PairVarcharCallReplace);
	pair_varchar.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	testpair.AddFunction(std::move(pair_varchar));
	testpair.DeclareLanguage();
	loader.RegisterFunction(std::move(testpair));

	// a language whose declaration arrives with overloads added to an existing undeclared function
	ScalarFunction merge_plain("testmerge",
	                           vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                           LogicalType::VARCHAR, InterpretFunction);
	merge_plain.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(std::move(merge_plain));
	ScalarFunction merge_language("testmerge", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                              InterpretFunction, nullptr, nullptr, nullptr, LogicalType::ANY);
	merge_language.SetCallReplaceCallback(TestLangCallReplace);
	merge_language.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	ScalarFunctionSet testmerge("testmerge");
	testmerge.AddFunction(std::move(merge_language));
	testmerge.DeclareLanguage();
	testmerge.SetCreateValidateCallback(TestLangCreateValidate);
	loader.RegisterFunction(std::move(testmerge));

	// ... and the same through AddFunctionOverload
	ScalarFunction late_plain("testlate",
	                          vector<LogicalType> {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                          LogicalType::VARCHAR, InterpretFunction);
	late_plain.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	loader.RegisterFunction(std::move(late_plain));
	ScalarFunction late_language("testlate", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                             InterpretFunction, nullptr, nullptr, nullptr, LogicalType::ANY);
	late_language.SetCallReplaceCallback(TestLangCallReplace);
	late_language.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	ScalarFunctionSet testlate("testlate");
	testlate.AddFunction(std::move(late_language));
	testlate.DeclareLanguage();
	testlate.SetCreateValidateCallback(TestLangCreateValidate);
	loader.AddFunctionOverload(std::move(testlate));

	// a language whose name another test extension also uses for an unrelated function in its own schema
	ScalarFunction plang_function("plang", vector<LogicalType> {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                              InterpretFunction, nullptr, nullptr, nullptr, LogicalType::ANY);
	plang_function.SetCallReplaceCallback(PlangCallReplace);
	ScalarFunctionSet plang("plang");
	plang.AddFunction(std::move(plang_function));
	plang.DeclareLanguage();
	loader.RegisterFunction(std::move(plang));

	// an aliased collated type, so tests pin that signature rendering preserves type identity
	loader.RegisterType("ci_text", LogicalType::VARCHAR_COLLATION("nocase").WithAlias("ci_text"));
}
}
