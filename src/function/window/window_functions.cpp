#include "duckdb/function/window/window_functions.hpp"

namespace duckdb {

void BuiltinFunctions::RegisterWindowFunctions() {
	Register<WindowFunctions>();
}

void WindowFunctions::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(RowNumberFunc::GetFunction());
	set.AddFunction(RankFunc::GetFunction());
	set.AddFunction(DenseRankFun::GetFunction());
	set.AddFunction(RankDenseFun::GetFunction());
	set.AddFunction(NtileFunc::GetFunction());
	set.AddFunction(PercentRankFun::GetFunction());
	set.AddFunction(CumeDistFun::GetFunction());
	set.AddFunction(FirstValueFun::GetFunction());
	set.AddFunction(LastValueFun::GetFunction());
	set.AddFunction(NthValueFun::GetFunction());
	set.AddFunction(LeadFun::GetFunctions());
	set.AddFunction(LagFun::GetFunctions());
	set.AddFunction(FillFun::GetFunction());
}

} // namespace duckdb
