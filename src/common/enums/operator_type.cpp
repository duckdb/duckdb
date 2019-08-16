#include "common/enums/operator_type.hpp"

using namespace std;

namespace duckdb {

string OperatorTypeToOperator(OperatorType type) {
	switch(type) {
	case OperatorType::ADD:
		return "+";
	case OperatorType::SUBTRACT:
		return "-";
	case OperatorType::MULTIPLY:
		return "*";
	case OperatorType::DIVIDE:
		return "/";
	case OperatorType::MOD:
		return "%";
	case OperatorType::BITWISE_LSHIFT:
		return "<<";
	case OperatorType::BITWISE_RSHIFT:
		return ">>";
	case OperatorType::BITWISE_AND:
		return "&";
	case OperatorType::BITWISE_OR:
		return "|";
	case OperatorType::BITWISE_XOR:
		return "#";
	default:
		return "";
	}
}

OperatorType OperatorTypeFromOperator(string op) {
	if (op == "+") {
		return OperatorType::ADD;
	} else if (op == "-") {
		return OperatorType::SUBTRACT;
	} else if (op == "*") {
		return OperatorType::MULTIPLY;
	} else if (op == "/") {
		return OperatorType::DIVIDE;
	} else if (op == "%") {
		return OperatorType::MOD;
	} else if (op == "<<") {
		return OperatorType::BITWISE_LSHIFT;
	} else if (op == ">>") {
		return OperatorType::BITWISE_RSHIFT;
	} else if (op == "&") {
		return OperatorType::BITWISE_AND;
	} else if (op == "|") {
		return OperatorType::BITWISE_OR;
	} else if (op == "#") {
		return OperatorType::BITWISE_XOR;
	}
	return OperatorType::NONE;
}

}
