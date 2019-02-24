#include "main/appender.hpp"
#include "config.h"
#include "porting.h"

#include "tdefs.h"

namespace tpcds {

struct tpcds_append_information {
	std::unique_ptr<duckdb::Appender> appender;
	tdef *table_def;
	size_t col;
	size_t row;
};

} // namespace tpcds
