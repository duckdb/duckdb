#include "icu-extension.hpp"
#include "icu-collate.hpp"

#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

struct IcuBindData : public FunctionData {
	std::unique_ptr<icu::Collator> collator;
	string language;
	string country;

	IcuBindData(string language_p, string country_p) :
		language(move(language_p)), country(move(country_p)) {
		UErrorCode status = U_ZERO_ERROR;
		this->collator = std::unique_ptr<icu::Collator>(icu::Collator::createInstance(icu::Locale(language.c_str(), country.c_str()), status));
		if (U_FAILURE(status)) {
			throw Exception("Failed to create ICU collator!");
		}
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<IcuBindData>(language.c_str(), country.c_str());
	}
};

static void icu_collate_function(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (IcuBindData &)*func_expr.bind_info;
	auto &collator = *info.collator;

	unique_ptr<char[]> buffer;
	int32_t buffer_size = 0;
	UnaryExecutor::Execute<string_t, string_t, true>(args.data[0], result, args.size(), [&](string_t input) {
		// create a sort key from the string
		int32_t string_size = collator.getSortKey(icu::UnicodeString::fromUTF8(icu::StringPiece(input.GetData(), input.GetSize())), (uint8_t*) buffer.get(), buffer_size);
		if (string_size > buffer_size) {
			// have to resize the buffer
			buffer_size = string_size + 1;
			buffer = unique_ptr<char[]>(new char[buffer_size]);

			string_size = collator.getSortKey(icu::UnicodeString::fromUTF8(icu::StringPiece(input.GetData(), input.GetSize())), (uint8_t*) buffer.get(), buffer_size);
		}
		return StringVector::AddBlob(result, string_t(buffer.get(), buffer_size));
	});
}

static unique_ptr<FunctionData> icu_collate_bind(BoundFunctionExpression &expr, ClientContext &context) {
	auto splits = StringUtil::Split(expr.function.name, "_");
	if (splits.size() == 1) {
		return make_unique<IcuBindData>(splits[0], "");
	} else if (splits.size() == 2) {
		return make_unique<IcuBindData>(splits[0], splits[1]);
	} else {
		throw InternalException("Expected one or two splits");
	}
}

static ScalarFunction get_icu_function(string collation) {
	return ScalarFunction(collation, {SQLType::VARCHAR}, SQLType::BIGINT, icu_collate_function, false, icu_collate_bind);
}

void ICUExtension::Load(DuckDB &db) {
	// load the collations
	Connection con(db);
	con.BeginTransaction();

	// iterate over all the collations
	int32_t count;
	auto locales = icu::Collator::getAvailableLocales(count);
	for(int32_t i = 0; i < count; i++) {
		string collation;
		if (string(locales[i].getCountry()).empty()) {
			// language only
			collation = locales[i].getLanguage();
		} else {
			// language + country
			collation = locales[i].getLanguage() + string("_") + locales[i].getCountry();
		}
		collation = StringUtil::Lower(collation);

		CreateCollationInfo info(collation, get_icu_function(collation), false, true);
		info.on_conflict = OnCreateConflict::IGNORE;
		db.catalog->CreateCollation(*con.context, &info);
	}

	con.Commit();
}

}
