#include "duckdb/common/types/geometry_crs.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"

#include "yyjson.hpp"
#include "fast_float/fast_float.h"

#include <limits>
#include <cmath>
#include <sys/stat.h>

namespace duckdb {

void CoordinateReferenceSystem::Parse(const string &text, CoordinateReferenceSystem &result) {
	if (text.empty()) {
		result.type = CoordinateReferenceSystemType::INVALID;
		return;
	}

	// Check if the text is all whitespace
	auto all_space = true;
	for (const auto c : text) {
		if (!StringUtil::CharacterIsSpace(c)) {
			all_space = false;
			break;
		}
	}

	if (all_space) {
		result.type = CoordinateReferenceSystemType::INVALID;
		return;
	}

	if (TryParsePROJJSON(text, result)) {
		return;
	}

	if (TryParseAuthCode(text, result)) {
		return;
	}

	// TODO: Also strip formatting
	if (TryParseWKT2(text, result)) {
		return;
	}

	// Otherwise, treat this as an opaque SRID identifier, and don't set an explicit name or id
	result.type = CoordinateReferenceSystemType::SRID;
	result.text = text;
}

bool CoordinateReferenceSystem::TryParse(const string &text, CoordinateReferenceSystem &result) {
	try {
		Parse(text, result);
	} catch (const InvalidInputException &ex) {
		return false;
	}
	return true;
}

CoordinateReferenceSystem::CoordinateReferenceSystem(const string &crs) {
	Parse(crs, *this);
}

void CoordinateReferenceSystem::Serialize(Serializer &serializer) const {
	// Only serialize the text definition
	serializer.WritePropertyWithDefault<string>(100, "text", text);
}

CoordinateReferenceSystem CoordinateReferenceSystem::Deserialize(Deserializer &deserializer) {
	string text;
	deserializer.ReadPropertyWithDefault<string>(100, "text", text);
	CoordinateReferenceSystem result;
	// If this fails for whatever reason, just return an invalid CRS
	if (!TryParse(text, result)) {
		result.text = "";
		result.type = CoordinateReferenceSystemType::INVALID;
	}
	return result;
}

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

		// Skip leading whitespace
		parser.SkipWhitespace();

		// Parse the root node
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
			if (pos == end) {
				throw InvalidInputException("Expected '%c' but got end of input at position %zu", c, pos - beg);
			}
			throw InvalidInputException("Expected '%c' but got '%c' at position %zu", c, *pos, pos - beg);
		}
	}

	bool TryMatchText(string &result) {
		const auto start = pos;
		while (pos < end && (isalpha(*pos) || *pos == '_')) {
			pos++;
		}
		if (pos == start) {
			// Didn't match any text
			return false;
		}
		result = string(start, UnsafeNumericCast<size_t>(pos - start));

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
		auto result = string(start, UnsafeNumericCast<size_t>(pos - start));

		Match('"');
		SkipWhitespace();

		return make_uniq<WKTString>(result);
	}

	unique_ptr<WKTValue> ParseKeywordNode() {
		string name;
		vector<unique_ptr<WKTValue>> children;

		if (!TryMatchText(name)) {
			return nullptr;
		}

		if (TryMatch('[')) {
			do {
				auto child = ParseNode();
				if (!child) {
					break;
				}
				children.push_back(std::move(child));
			} while (TryMatch(','));

			Match(']');
			return make_uniq<WKTKeyword>(std::move(name), std::move(children));
		}

		if (TryMatch('(')) {
			do {
				auto child = ParseNode();
				if (!child) {
					break;
				}
				children.push_back(std::move(child));
			} while (TryMatch(','));
			Match(')');
			return make_uniq<WKTKeyword>(std::move(name), std::move(children));
		}

		// Nodes don't have to have children
		return make_uniq<WKTKeyword>(std::move(name), std::move(children));
	}

	unique_ptr<WKTValue> ParseNumberNode() {
		if (pos != end) {
			double num;
			const auto res = duckdb_fast_float::from_chars(pos, end, num);
			if (res.ec != std::errc()) {
				return nullptr;
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

static string TryExtractIDFromWKTNode(const WKTKeyword &keyword) {
	auto &children = keyword.GetChildren();
	for (const auto &child : children) {
		if (!child->IsKeyword()) {
			continue;
		}

		const auto &child_keyword = child->As<WKTKeyword>();
		if (!child_keyword.Match("ID")) {
			continue;
		}

		auto &id_children = child_keyword.GetChildren();
		if (id_children.size() < 2) {
			continue;
		}

		const auto &first = id_children[0];
		const auto &second = id_children[1];

		if (!first->IsString()) {
			continue;
		}

		const auto code_auth = first->As<WKTString>().GetValue();
		if (second->IsNumber()) {
			const auto number = second->As<WKTNumber>().GetValue();
			if (number < 0 || number > static_cast<double>(std::numeric_limits<idx_t>::max())) {
				continue;
			}
			return code_auth + ":" + StringUtil::Format("%llu", static_cast<idx_t>(number));
		}
		if (second->IsString()) {
			return code_auth + ":" + second->As<WKTString>().GetValue();
		}
		return string();
	}

	return string();
}
bool CoordinateReferenceSystem::TryParseWKT2(const string &text, CoordinateReferenceSystem &result) {
	const auto node = WKTParser::Parse(text);

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

	const auto crs_keywords = {
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

		// TODO: Parse "ID" subnode to get the code
		auto name = TryExtractIDFromWKTNode(transform_keyword);
		if (name.empty()) {
			// Pick name as fallback instead of code
			name = first_transform_child->As<WKTString>().GetValue();
		}

		result.type = CoordinateReferenceSystemType::WKT2_2019;
		result.name = name;
		result.text = text;

		// Also trim text
		// TODO: Normalize WKT Input
		StringUtil::Trim(result.text);

		return true;
	}

	// Otherwise, the name is the first child
	const auto &first = children[0];
	if (!first->IsString()) {
		// Pick name as fallback instead of code
		return false;
	}

	auto name = TryExtractIDFromWKTNode(keyword);
	if (name.empty()) {
		// Pick name as fallback
		name = first->As<WKTString>().GetValue();
	}
	result.name = name;
	result.type = CoordinateReferenceSystemType::WKT2_2019;
	result.text = text;

	// Also trim text
	// TODO: Normalize WKT Input
	StringUtil::Trim(result.text);

	return true;
}

//----------------------------------------------------------------------------------------------------------------------
// PROJJSON Parsing
//----------------------------------------------------------------------------------------------------------------------
bool CoordinateReferenceSystem::TryParsePROJJSON(const string &text, CoordinateReferenceSystem &result) {
	using namespace duckdb_yyjson; // NOLINT

	unique_ptr<yyjson_doc, void (*)(yyjson_doc *)> doc(yyjson_read(text.c_str(), text.size(), YYJSON_READ_NOFLAG),
	                                                   yyjson_doc_free);

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
	const auto projjson_crs_types = {"GeographicCRS", "GeodeticCRS",    "ProjectedCRS", "CompoundCRS",  "BoundCRS",
	                                 "VerticalCRS",   "EngineeringCRS", "TemporalCRS",  "ParametricCRS"};

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

	// Start out with the root object
	yyjson_val *target_val = root;

	// Special case for BoundCRS, use the name of the transformation instead
	if (StringUtil::CIEquals(type_str, "BoundCRS")) {
		const auto trans_val = yyjson_obj_get(root, "transformation");
		if (!trans_val || !yyjson_is_obj(trans_val)) {
			return false;
		}

		// Switch to the transformation object
		target_val = trans_val;
	}

	// Try to get the "name" field from the target object
	yyjson_val *name_val = yyjson_obj_get(target_val, "name");
	if (name_val && yyjson_is_str(name_val)) {
		const char *name_str = yyjson_get_str(name_val);
		if (name_str) {
			result.name = string(name_str);
		}
	}

	// Try to get the "id" field from the target object
	yyjson_val *id_val = yyjson_obj_get(target_val, "id");
	if (id_val && yyjson_is_obj(id_val)) {
		const auto auth_val = yyjson_obj_get(id_val, "authority");
		if (auth_val && yyjson_is_str(auth_val)) {
			const auto auth_str = yyjson_get_str(auth_val);

			if (auth_str) {
				result.code = string(auth_str);

				const auto code_val = yyjson_obj_get(id_val, "code");
				if (code_val && yyjson_is_int(code_val)) {
					const auto code_int = yyjson_get_int(code_val);
					result.code += ":" + StringUtil::Format("%d", code_int);
				}
				if (code_val && yyjson_is_str(code_val)) {
					const auto code_str = yyjson_get_str(code_val);
					if (code_str) {
						result.code += ":" + string(code_str);
					}
				}
			}
		}
	}

	result.type = CoordinateReferenceSystemType::PROJJSON;

	// Print the PROJJSON back to a string to normalize it
	// TODO: We should actually normalize the PROJJSON here (e.g. sort fields) to ensure consistent equality checks
	size_t json_size = 0;
	const auto json_text = yyjson_write(doc.get(), YYJSON_WRITE_NOFLAG, &json_size);
	if (!json_text) {
		return false;
	}

	result.text = string(json_text, json_size);
	free(json_text);

	return true;
}

//----------------------------------------------------------------------------------------------------------------------
// AUTH:CODE Parsing
//----------------------------------------------------------------------------------------------------------------------
bool CoordinateReferenceSystem::TryParseAuthCode(const string &text, CoordinateReferenceSystem &result) {
	auto beg = text.c_str();
	auto end = beg + text.size();

	// Remove whitespace
	while (beg != end) {
		if (!StringUtil::CharacterIsSpace(*beg)) {
			break;
		}
		beg++;
	}
	while (end != beg) {
		if (!StringUtil::CharacterIsSpace(*(end - 1))) {
			break;
		}
		end--;
	}

	for (auto colon_pos = beg; colon_pos != end; colon_pos++) {
		if (*colon_pos == ':') {
			bool auth_valid = true;
			for (auto ptr = beg; ptr != colon_pos; ptr++) {
				if (!StringUtil::CharacterIsAlpha(*ptr)) {
					auth_valid = false;
					break;
				}
			}

			bool code_valid = true;
			for (auto ptr = colon_pos + 1; ptr != end; ptr++) {
				if (!StringUtil::CharacterIsAlphaNumeric(*ptr)) {
					code_valid = false;
					break;
				}
			}

			if (auth_valid && code_valid) {
				// Valid AUTH:CODE
				result.type = CoordinateReferenceSystemType::AUTH_CODE;
				result.text = string(beg, UnsafeNumericCast<size_t>(end - beg));
				result.code = result.text;
				return true;
			}
			break;
		}
	}

	return false;
}

} // namespace duckdb
