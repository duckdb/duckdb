//===----------------------------------------------------------------------===//
//                         DuckDB
//
// object_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb
{

//! ObjectCache is the base class for objects caches in DuckDB
class ObjectCache{
public:
	virtual ~ObjectCache(){}
};
} // namespace duckdb
