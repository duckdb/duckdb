
#pragma once

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "common/printable.hpp"

template <typename T, typename... Args>
std::unique_ptr<T> make_unique(Args &&... args) {
	return std::unique_ptr<T>(new T(std::forward<Args>(args)...));
}

struct List;
struct Node;

class SQLStatement : public Printable {
  public:
	SQLStatement(StatementType type) : stmt_type(type){};
	virtual ~SQLStatement() {}

	StatementType GetType() const { return stmt_type; }

  private:
	StatementType stmt_type;
};
