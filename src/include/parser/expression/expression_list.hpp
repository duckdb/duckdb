//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/expression_list.hpp
//
// Author: Mark Raasveldt
// Description: This file contains a list of includes to include all
//              expressions, as they are used in several places.
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/basetableref_expression.hpp"
#include "parser/expression/cast_expression.hpp"
#include "parser/expression/columnref_expression.hpp"
#include "parser/expression/comparison_expression.hpp"
#include "parser/expression/conjunction_expression.hpp"
#include "parser/expression/constant_expression.hpp"
#include "parser/expression/crossproduct_expression.hpp"
#include "parser/expression/function_expression.hpp"
#include "parser/expression/groupref_expression.hpp"
#include "parser/expression/join_expression.hpp"
#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"
#include "parser/expression/tableref_expression.hpp"
