#include "dsdgen_helpers.hpp"

#define DECLARER
#include "build_support.h"
#include "params.h"

#include "tdefs.h"
#include "scaling.h"
#include "address.h"
#include "dist.h"
#include "genrand.h"

#include "config.h"
#include "porting.h"

#include "tdefs.h"

namespace tpcds {

void InitializeDSDgen() {
	init_params(); // among other set random seed
	init_rand();   // no random numbers without this
}

ds_key_t GetRowCount(int table_id) {
	return get_rowcount(table_id);
}

void ResetCountCount() {
	resetCountCount();
}

tpcds_table_def GetTDefByNumber(int table_id) {
	auto tdef = getSimpleTdefsByNumber(table_id);
	tpcds_table_def def;
	def.name = tdef->name;
	def.fl_child = tdef->flags & FL_CHILD ? 1 : 0;
	def.fl_small = tdef->flags & FL_SMALL ? 1 : 0;
	return def;
}

tpcds_builder_func GetTDefFunctionByNumber(int table_id) {
	auto table_funcs = getTdefFunctionsByNumber(table_id);
	return table_funcs->builder;
}

} // namespace tpcds
