#include "duckdb/common/json/json_value.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

//------------------------------------------------------
// ::Parse()
//------------------------------------------------------
class JsonParser {
	friend class JsonValue;
	const char *start;
	const char *pos;
	const char *end;
	idx_t line;
	idx_t recursion_depth;

private:
	explicit JsonParser(const string &str)
	    : start(str.c_str()), pos(str.c_str()), end(str.c_str() + str.size()), line(1), recursion_depth(0) {
	}

	[[noreturn]] void Error(const char *msg) {
		auto prefix = StringUtil::Format("JsonParser: Error at line %d, (byte position: %d): ", line, pos - start);
		throw SerializationException(msg);
	}

	char Next() {
		if (pos >= end) {
			return 0;
		}
		if (*pos == '\n') {
			line++;
		}
		return *pos++;
	}
	char Peek() {
		if (pos >= end) {
			return 0;
		}
		return *pos;
	}

	bool Match(char c) {
		if (*pos == c) {
			pos++;
			return true;
		}
		return false;
	}

	bool Match(const char *str) {
		auto beg = pos;
		while (*str) {
			if (*str != *pos) {
				// Reset the position
				pos = beg;
				return false;
			}
			str++;
			pos++;
		}
		return true;
	}

	void MatchWhiteSpace() {
		while (*pos == ' ' || *pos == '\t' || *pos == '\n' || *pos == '\r') {
			if (*pos == '\n') {
				line++;
			}
			pos++;
		}
	}

	// Like match, but also skips leading whitespace
	bool MatchToken(const char *str) {
		MatchWhiteSpace();
		return Match(str);
	}

	bool MatchToken(char c) {
		MatchWhiteSpace();
		return Match(c);
	}

	JsonValue ParseValue() {
		if (recursion_depth > 1000) {
			Error("Recursion depth exceeded maximum depth of 1000");
		}

		MatchWhiteSpace();
		auto c = Next();
		switch (c) {
		case '{': {
			JsonValue obj(JsonKind::OBJECT);
			if (MatchToken('}')) {
				return obj;
			}
			while (true) {
				auto key = ParseValue();
				if (key.GetType() != JsonKind::STRING) {
					Error("Expected string key in object");
				}
				if (!MatchToken(":")) {
					Error("Expected colon after key in object");
				}
				auto value = ParseValue();
				obj.Emplace(key.As<string>(), std::move(value));
				if (!MatchToken(',')) {
					break;
				}
			}
			if (!MatchToken('}')) {
				Error("Expected closing brace after object");
			}
			return obj;
		}
		case '[': {
			JsonValue arr(JsonKind::ARRAY);
			if (MatchToken(']')) {
				return arr;
			}
			while (true) {
				auto value = ParseValue();
				arr.Emplace(std::move(value));
				if (!MatchToken(',')) {
					break;
				}
			}
			if (!MatchToken(']')) {
				Error("Expected closing bracket after array");
			}
			return arr;
		}
		case 'n': {
			if (!Match("ull")) {
				Error("Found 'n' but expected 'null'");
			}
			return JsonValue(JsonKind::NULLVALUE);
		}
		case 't': {
			if (!Match("rue")) {
				Error("Found 't' but expected 'true'");
			}
			return JsonValue(true);
		}
		case 'f': {
			if (!Match("alse")) {
				Error("Found 'f' but expected 'false'");
			}
			return JsonValue(false);
		}
		case '"': {
			string str;
			while (true) {
				c = Next();
				if (c == 0) {
					Error("Expected closing quote");
				} else if (c == '\\') {
					c = Next();
					switch (c) {
					case '"':
						str += '"';
						break;
					case '\\':
						str += '\\';
						break;
					case '/':
						str += '/';
						break;
					case 'b':
						str += '\b';
						break;
					case 'f':
						str += '\f';
						break;
					case 'n':
						str += '\n';
						break;
					case 'r':
						str += '\r';
						break;
					case 't':
						str += '\t';
						break;
					case '0':
						str += '\0';
						break;
					case 'u':
						Error("Unicode escape sequences are not supported");
					default:
						Error("Invalid escape sequence");
					}
				} else if (c == '"') {
					break;
				} else {
					str += c;
				}
			}
			return JsonValue(str);
		}
		case '-':
		case '0':
		case '1':
		case '2':
		case '3':
		case '4':
		case '5':
		case '6':
		case '7':
		case '8':
		case '9': {
			string str;
			if (c == '-') {
				str += '-';
				c = Next();
			}
			if (c == '0') {
				str += '0';
			} else if (c >= '1' && c <= '9') {
				str += c;
				c = Peek();
				while (c >= '0' && c <= '9') {
					str += Next();
					c = Peek();
				}
			}

			if (Peek() == '.') {
				str += '.';
				c = Next();
				if (c < '0' || c > '9') {
					Error("Expected digit after decimal point");
				}
				while (c >= '0' && c <= '9') {
					str += Next();
					c = Peek();
				}
			}
			c = Peek();
			if (c == 'e' || c == 'E') {
				str += c;
				c = Peek();
				if (c == '+' || c == '-') {
					str += Next();
				}
				c = Peek();
				if (c < '0' || c > '9') {
					Error("Expected digit after exponent");
				}
				while (c >= '0' && c <= '9') {
					str += Next();
					c = Peek();
				}
			}
			return JsonValue(std::atof(str.c_str()));
		}
		default:
			Error("Unexpected character");
		}
	}
};

JsonValue JsonValue::Parse(const string &str) {
	JsonParser reader(str);
	return reader.ParseValue();
}

//------------------------------------------------------
// ::ToString()
//------------------------------------------------------

static string ToStringInternal(const JsonValue &value, bool format, idx_t level) {
	switch (value.GetType()) {
	case JsonKind::STRING: {
		auto &str = value.As<string>();
		string result = "\"";
		for (auto c : str) {
			switch (c) {
			case '\0':
				result += "\\0";
				break;
			case '"':
				result += "\\\"";
				break;
			case '\\':
				result += "\\\\";
				break;
			case '\b':
				result += "\\b";
				break;
			case '\f':
				result += "\\f";
				break;
			case '\n':
				result += "\\n";
				break;
			case '\r':
				result += "\\r";
				break;
			case '\t':
				result += "\\t";
				break;
			default:
				result += c;
				break;
			}
		}
		result += '"';
		return result;
	}
	case JsonKind::NUMBER:
		return std::to_string(value.As<double>());
	case JsonKind::BOOLEAN:
		return value.As<bool>() ? "true" : "false";
	case JsonKind::NULLVALUE:
		return "null";
	case JsonKind::OBJECT: {
		string result = "{";
		if (format) {
			result += "\n";
		}
		idx_t count = 0;
		for (auto &entry : value.Properties()) {
			if (format) {
				for (idx_t i = 0; i < level + 1; i++) {
					result += "\t";
				}
			}
			result += "\"" + entry.first + "\": " + ToStringInternal(entry.second, format, level + 1);
			if (++count < value.Count()) {
				result += ",";
			}
			if (format) {
				result += "\n";
			}
		}
		if (format) {
			for (idx_t i = 0; i < level; i++) {
				result += "\t";
			}
		}
		result += "}";
		return result;
	}
	case JsonKind::ARRAY: {
		string result = "[";
		if (format) {
			result += "\n";
		}
		idx_t count = 0;
		for (auto &entry : value.Items()) {
			if (format) {
				for (idx_t i = 0; i < level + 1; i++) {
					result += "\t";
				}
			}
			result += ToStringInternal(entry, format, level + 1);
			if (++count < entry.Count()) {
				result += ",";
			}
			if (format) {
				result += "\n";
			}
		}
		if (format) {
			for (idx_t i = 0; i < level; i++) {
				result += "\t";
			}
		}
		result += "]";
		return result;
	}
	default:
		throw InvalidInputException("Unrecognized JSON kind!");
	}
}

string JsonValue::ToString(bool format) const {
	return ToStringInternal(*this, format, 0);
}

} // namespace duckdb
