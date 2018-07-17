
#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
	class FunctionExpression : public AbstractExpression {
	  public:
		FunctionExpression(
		    const char *func_name,
		    std::vector<std::unique_ptr<AbstractExpression>> &children)
		    : AbstractExpression(ExpressionType::FUNCTION), func_name(func_name) {
			for (auto &child : children) {
				this->children.push_back(std::move(child));
			}
		}

		virtual std::string ToString() const { return std::string(); }

	  private:
		std::string func_name;
		std::vector<TypeId> func_arg_types;
	};
}
