#include "duckdb/common/types/geometry_crs.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/string_util.hpp"

#include "yyjson.hpp"
#include "fast_float/fast_float.h"

#include <limits>
#include <cmath>

namespace duckdb {

//----------------------------------------------------------------------------------------------------------------------
// WKT2:2019 Parsing
//----------------------------------------------------------------------------------------------------------------------

class WKTKeyword;
class WKTNumber;
class WKTString;

class WKTValue {
public:
	enum class Type { KEYWORD, NUMBER, STRING } type;
	virtual ~WKTValue() = default;

	template <class T>
	T &As() {
		D_ASSERT(T::TYPE == type);
		return reinterpret_cast<T &>(*this);
	}

	template <class T>
	T &As() const {
		D_ASSERT(T::TYPE == type);
		return reinterpret_cast<T &>(*this);
	}

	bool IsKeyword() const {
		return type == Type::KEYWORD;
	}
	bool IsNumber() const {
		return type == Type::NUMBER;
	}
	bool IsString() const {
		return type == Type::STRING;
	}

protected:
	explicit WKTValue(Type type_p) : type(type_p) {
	}
};

class WKTKeyword final : public WKTValue {
public:
	static constexpr auto TYPE = Type::KEYWORD;

	const string &GetName() const {
		return name;
	}

	const vector<unique_ptr<WKTValue>> &GetChildren() const {
		return children;
	}

	bool Match(const char *keyword) const {
		return StringUtil::CIEquals(name, keyword);
	}

	explicit WKTKeyword(string name, vector<unique_ptr<WKTValue>> children_p)
	    : WKTValue(TYPE), name(std::move(name)), children(std::move(children_p)) {
	}

private:
	string name;
	vector<unique_ptr<WKTValue>> children;
};

class WKTNumber final : public WKTValue {
public:
	static constexpr auto TYPE = Type::NUMBER;

	double GetValue() const {
		return value;
	}

	explicit WKTNumber(double value_p) : WKTValue(TYPE), value(value_p) {
	}

private:
	double value;
};

class WKTString final : public WKTValue {
public:
	static constexpr auto TYPE = Type::STRING;

	const string &GetValue() const {
		return value;
	}

	explicit WKTString(string value) : WKTValue(TYPE), value(std::move(value)) {
	}

private:
	string value;
};

class WKTParser {
public:
	static unique_ptr<WKTValue> Parse(const string &wkt) {
		WKTParser parser(wkt.c_str(), wkt.size());
		return parser.ParseNode();
	}

private:
	const char *beg;
	const char *end;
	const char *pos;

private:
	WKTParser(const char *text, size_t size) : beg(text), end(text + size), pos(text) {
		SkipWhitespace();
	}

	bool TryMatch(char c) {
		if (pos < end && tolower(*pos) == tolower(c)) {
			pos++;
			SkipWhitespace(); // remove trailing whitespace
			return true;
		}
		return false; // not matched
	}

	void Match(char c) {
		if (!TryMatch(c)) {
			throw InvalidInputException("Expected '%c' but got '%c' at position %zu", c, *pos, pos - beg);
		}
	}

	bool TryMatchText(string &result) {
		const auto start = pos;
		while (pos < end && (isalpha(*pos) || *pos == '_')) {
			pos++;
		}
		if (pos == beg) {
			// Didnt match any text
			return false;
		}
		result = string(start, pos - start);

		SkipWhitespace();
		return true;
	}

	unique_ptr<WKTValue> ParseStringNode() {
		if (!TryMatch('"')) {
			return nullptr;
		}
		const char *start = pos;
		while (pos < end && *pos != '"') {
			pos++;
		}
		if (pos == end) {
			throw InvalidInputException("Unterminated string starting at position %zu", start - beg);
		}
		auto result = string(start, pos - start);

		Match('"');
		SkipWhitespace();

		return make_uniq<WKTString>(result);
	}

	unique_ptr<WKTValue> ParseKeywordNode() {
		string name;
		if (!TryMatchText(name)) {
			return nullptr;
		}

		if (TryMatch('[')) {
			vector<unique_ptr<WKTValue>> children;
			do {
				auto child = ParseNode();
				if (!child) {
					break;
				}
				children.push_back(std::move(child));
			} while (TryMatch(','));

			Match(']');
			return make_uniq<WKTKeyword>(std::move(children));
		}

		if (TryMatch('(')) {
			vector<unique_ptr<WKTValue>> children;
			do {
				auto child = ParseNode();
				if (!child) {
					break;
				}
				children.push_back(std::move(child));
			} while (TryMatch(','));
			Match(')');
			return make_uniq<WKTKeyword>(std::move(children));
		}

		throw InvalidInputException("Expected '[' or '(' after WKT keyword at position %zu", pos - beg);
	}

	unique_ptr<WKTValue> ParseNumberNode() {
		if (pos != end && isdigit(*pos)) {
			double num;
			const auto res = duckdb_fast_float::from_chars(pos, end, num);
			if (res.ec != std::errc()) {
				throw InvalidInputException("Expected number at position %zu", pos - beg);
			}
			pos = res.ptr;    // update position to the end of the parsed number
			SkipWhitespace(); // remove trailing whitespace
			return make_uniq<WKTNumber>(num);
		}

		return nullptr;
	}

	unique_ptr<WKTValue> ParseNode() {
		unique_ptr<WKTValue> node = nullptr;

		node = ParseStringNode();
		if (node) {
			return node;
		}

		node = ParseKeywordNode();
		if (node) {
			return node;
		}

		node = ParseNumberNode();
		if (node) {
			return node;
		}

		return nullptr;
	}

	void SkipWhitespace() {
		while (pos < end && isspace(*pos)) {
			pos++;
		}
	}
};

static bool TryIdentifyWKT2(const string &wkt2, string &name) {
	const auto node = WKTParser::Parse(wkt2);

	if (!node) {
		// Not a valid WKT2 string
		return false;
	}

	if (!node->IsKeyword()) {
		return false;
	}

	const auto &keyword = node->As<WKTKeyword>();

	// Check that this is a valid CRS keyword
	auto found = false;

	constexpr auto crs_keywords = {
	    // WKT2019 keywords
	    "BOUNDCRS",     "COMPOUNDCRS", "ENGCRS",        "ENGINEERINGCRS", "GEODCRS",
	    "GEODETICCRS",  "GEOGCRS",     "GEOGRAPHICCRS", "PARAMETRICCRS",  "PROJCRS",
	    "PROJECTEDCRS", "TIMECRS",     "VERTCRS",       "VERTICALCRS"
	    // Older WKT1 keywords (we dont support these for now)
	    // "COMPDCS",
	    // "FITTED_CS",
	    // "GEOCCS",
	    // "GEOGCS",
	    // "LOCAL_CS",
	    // "PROJCS",
	    // "VERT_CS",
	};

	// Check if we match any of the CRS keywords
	for (auto &kw : crs_keywords) {
		if (keyword.Match(kw)) {
			found = true;
			break;
		}
	}

	if (!found) {
		// Not a valid CRS keyword
		return false;
	}

	auto &children = keyword.GetChildren();
	if (children.empty()) {
		// Needs a name
		return false;
	}

	// Special case for BOUNDCRS, use the name of the transform instead
	if (keyword.Match("BOUNDCRS")) {
		if (children.size() < 3) {
			// Needs at least source CRS, target CRS and transform
			return false;
		}

		const auto &transform_node = children[2];
		if (!transform_node->IsKeyword()) {
			// Transform needs to be a keyword
			return false;
		}

		const auto &transform_keyword = transform_node->As<WKTKeyword>();
		auto &transform_children = transform_keyword.GetChildren();
		if (transform_children.empty()) {
			// Transform needs a name
			return false;
		}

		const auto &first_transform_child = transform_children[0];
		if (!first_transform_child->IsString()) {
			// First child of transform needs to be a string (the name)
			return false;
		}

		name = first_transform_child->As<WKTString>().GetValue();
		return true;
	}

	// Otherwise, the name is the first child
	const auto &first = children[0];
	if (!first->IsString()) {
		// First child needs to be a string (the name)
		return false;
	}

	name = first->As<WKTString>().GetValue();
	return true;
}

//----------------------------------------------------------------------------------------------------------------------
// PROJJSON Parsing
//----------------------------------------------------------------------------------------------------------------------
static bool TryIdentifyPROJJSON(const string &projjson, string &name) {
	using namespace duckdb_yyjson;

	unique_ptr<yyjson_doc, void (*)(yyjson_doc *)> doc(
	    yyjson_read(projjson.c_str(), projjson.size(), YYJSON_READ_NOFLAG), yyjson_doc_free);

	if (!doc) {
		// Not a valid JSON
		return false;
	}

	yyjson_val *root = yyjson_doc_get_root(doc.get());
	if (!root || !yyjson_is_obj(root)) {
		// The root is not an object
		return false;
	}

	// Get the "type" field from the root object
	yyjson_val *type_val = yyjson_obj_get(root, "type");
	if (!type_val || !yyjson_is_str(type_val)) {
		return false;
	}

	// Check that the type is one of the PROJJSON CRS types
	// There are other (derived CRS) types, but they can not be used as root CRS definitions
	const string type_str = yyjson_get_str(type_val);
	constexpr auto projjson_crs_types = {"GeographicCRS",
	                                     "GeodeticCRS",
	                                     "ProjectedCRS",
	                                     "CompoundCRS",
	                                     "BoundCRS",
	                                     "VerticalCRS",
	                                     "EngineeringCRS",
	                                     "TemporalCRS"
	                                     "ParametricCRS"};

	auto found = false;
	for (auto &kw : projjson_crs_types) {
		if (StringUtil::CIEquals(type_str, kw)) {
			found = true;
			break;
		}
	}

	if (!found) {
		return false;
	}

	// Special case for BoundCRS, use the name of the transformation instead
	if (StringUtil::CIEquals(type_str, "BoundCRS")) {
		yyjson_val *transformation_val = yyjson_obj_get(root, "transformation");
		if (!transformation_val || !yyjson_is_obj(transformation_val)) {
			return false;
		}
		yyjson_val *name_val = yyjson_obj_get(transformation_val, "name");
		if (!name_val || !yyjson_is_str(name_val)) {
			return false;
		}
		name = yyjson_get_str(name_val);
		return true;
	}

	// Try to get the "name" field from the root object
	yyjson_val *name_val = yyjson_obj_get(root, "name");
	if (name_val && yyjson_is_str(name_val)) {
		const char *name_str = yyjson_get_str(name_val);
		if (name_str) {
			name = string(name_str);
			return true;
		}
	}

	return false;
}

//----------------------------------------------------------------------------------------------------------------------
// AUTH:CODE Parsing
//----------------------------------------------------------------------------------------------------------------------
static bool TryIdentifyAuthCode(const string &code) {
	auto beg = code.c_str();
	auto end = beg + code.size();

	for (auto colon_pos = beg; colon_pos != end; colon_pos++) {
		if (*colon_pos == ':') {
			bool auth_valid = true;
			for (auto ptr = beg; ptr != colon_pos; ptr++) {
				if (!std::isalpha(*ptr)) {
					auth_valid = false;
					break;
				}
			}

			bool code_valid = true;
			for (auto ptr = colon_pos + 1; ptr != end; ptr++) {
				if (!std::isdigit(*ptr)) {
					code_valid = false;
					break;
				}
			}

			if (auth_valid && code_valid) {
				return true;
			}
			break;
		}
	}

	return false;
}

struct CoordinateSystemReferenceLookupResult {
	CoordinateReferenceSystemType type;
	string name;
	string text;
};

// TODO: Move this into pluggable util class
static CoordinateSystemReferenceLookupResult IdentifyCoordinateSystem(const string &text_p) {
	CoordinateSystemReferenceLookupResult result;
	result.text = text_p;

	// Trim whitespace
	StringUtil::Trim(result.text);

	if (result.text.empty()) {
		result.type = CoordinateReferenceSystemType::INVALID;
		return result;
	}

	if (TryIdentifyPROJJSON(result.text, result.name)) {
		result.type = CoordinateReferenceSystemType::PROJJSON;
		return result;
	}

	// Look for AUTH:CODE pattern
	if (TryIdentifyAuthCode(result.text)) {
		result.type = CoordinateReferenceSystemType::AUTH_CODE;
		return result;
	}

	// Look for WKT2 pattern
	if (TryIdentifyWKT2(result.text, result.name)) {
		result.type = CoordinateReferenceSystemType::WKT2_2019;
		return result;
	}

	// Otherwise, treat this as an opaque SRID identifier, and don't set an explicit name
	result.type = CoordinateReferenceSystemType::SRID;

	return result;
}

CoordinateReferenceSystem::CoordinateReferenceSystem(const string &crs) {
	const auto result = IdentifyCoordinateSystem(crs);
	type = result.type;
	name = result.name;
	text = crs;
}

} // namespace duckdb
