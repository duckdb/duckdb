#include <string>
#include <utility>

#include "duckdb/parser/statement/load_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/common/enums/logical_operator_type.hpp"
#include "duckdb/common/enums/statement_type.hpp"
#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/main/query_parameters.hpp"
#include "duckdb/parser/parsed_data/load_info.hpp"
#include "duckdb/planner/bound_statement.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

BoundStatement Binder::Bind(LoadStatement &stmt) {
	BoundStatement result;
	result.types = {LogicalType::BOOLEAN};
	result.names = {"Success"};

	// Ensure the repository exists if it's an alias
	if (!stmt.info->repository.empty() && stmt.info->repo_is_alias) {
		auto repository_url = ExtensionRepository::TryGetRepositoryUrl(stmt.info->repository);
		if (repository_url.empty()) {
			throw BinderException("'%s' is not a known repository name. Are you trying to query from a repository by "
			                      "path? Use single quotes: `FROM '%s'`",
			                      stmt.info->repository, stmt.info->repository);
		}
	}

	result.plan = make_uniq<LogicalSimple>(LogicalOperatorType::LOGICAL_LOAD, std::move(stmt.info));

	auto &properties = GetStatementProperties();
	properties.output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	properties.return_type = StatementReturnType::NOTHING;
	return result;
}

} // namespace duckdb
