#include "geo-functions.hpp"

#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "geometry.hpp"

namespace duckdb {

bool GeoFunctions::CastVarcharToGEO(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto constant = source.GetVectorType() == VectorType::CONSTANT_VECTOR;

	UnifiedVectorFormat vdata;
	source.ToUnifiedFormat(count, vdata);

	auto input = (string_t *)vdata.data;
	auto result_data = FlatVector::GetData<string_t>(result);
	bool success = true;
	for (idx_t i = 0; i < (constant ? 1 : count); i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		auto gser = Geometry::ToGserialized(input[idx]);
		if (!gser) {
			FlatVector::SetNull(result, i, true);
			success = false;
			continue;
		}
		idx_t rv_size = Geometry::GetGeometrySize(gser);
		string_t rv = StringVector::EmptyString(result, rv_size);
		Geometry::ToGeometry(gser, (data_ptr_t)rv.GetDataWriteable());
		Geometry::DestroyGeometry(gser);
		rv.Finalize();
		result_data[i] = rv;
	}
	if (constant) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
	return success;
}

bool GeoFunctions::CastGeoToVarchar(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	GenericExecutor::ExecuteUnary<PrimitiveType<string_t>, PrimitiveType<string_t>>(
	    source, result, count, [&](PrimitiveType<string_t> input) {
		    auto text = Geometry::GetString(input.val);
		    return StringVector::AddString(result, text);
	    });
	return true;
}

struct MakePointBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA point_x, TB point_y) {
		auto gser = Geometry::MakePoint(point_x, point_y);
		idx_t rv_size = Geometry::GetGeometrySize(gser);
		auto base = Geometry::GetBase(gser);
		Geometry::DestroyGeometry(gser);
		return string_t((const char *)base, rv_size);
	}
};

struct MakePointTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA point_x, TB point_y, TC point_z) {
		auto gser = Geometry::MakePoint(point_x, point_y, point_z);
		idx_t rv_size = Geometry::GetGeometrySize(gser);
		auto base = Geometry::GetBase(gser);
		Geometry::DestroyGeometry(gser);
		return string_t((const char *)base, rv_size);
	}
};

template <typename TA, typename TB, typename TR>
static void MakePointBinaryExecutor(Vector &point_x, Vector &point_y, Vector &result, idx_t count) {
	BinaryExecutor::ExecuteStandard<TA, TB, TR, MakePointBinaryOperator>(point_x, point_y, result, count);
}

template <typename TA, typename TB, typename TC, typename TR>
static void MakePointTernaryExecutor(Vector &point_x, Vector &point_y, Vector &point_z, Vector &result, idx_t count) {
	TernaryExecutor::Execute<TA, TB, TC, TR>(point_x, point_y, point_z, result, count,
	                                         MakePointTernaryOperator::Operation<TA, TB, TC, TR>);
}

void GeoFunctions::MakePointFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &point_x_arg = args.data[0];
	auto &point_y_arg = args.data[1];
	if (args.data.size() == 2) {
		MakePointBinaryExecutor<double, double, string_t>(point_x_arg, point_y_arg, result, args.size());
	} else if (args.data.size() == 3) {
		auto &point_z_arg = args.data[2];
		MakePointTernaryExecutor<double, double, double, string_t>(point_x_arg, point_y_arg, point_z_arg, result,
		                                                           args.size());
	}
}

struct AsTextUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text, Vector &result) {
		if (text.GetSize() == 0) {
			return text;
		}
		string str = Geometry::AsText((data_ptr_t)text.GetDataUnsafe(), text.GetSize());
		auto result_str = StringVector::EmptyString(result, str.size());
		memcpy(result_str.GetDataWriteable(), str.c_str(), str.size());
		result_str.Finalize();
		return result_str;
	}
};

template <typename TA, typename TR>
static void GeometryAsTextUnaryExecutor(Vector &text, Vector &result, idx_t count) {
	UnaryExecutor::ExecuteString<TA, TR, AsTextUnaryOperator>(text, result, count);
}

void GeoFunctions::GeometryAsTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
	GeometryAsTextUnaryExecutor<string_t, string_t>(text_arg, result, args.size());
}

struct GeometryDistanceBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA geom1, TB geom2) {
		double dis = 0.00;
		if (geom1.GetSize() == 0 || geom2.GetSize() == 0) {
			return dis;
		}
		auto gser1 = Geometry::GetGserialized(geom1);
		auto gser2 = Geometry::GetGserialized(geom2);
		if (!gser1 || !gser2) {
			throw ConversionException("Failure in geometry distance: could not calculate distance from geometries");
		}
		dis = Geometry::Distance(gser1, gser2);
		Geometry::DestroyGeometry(gser1);
		Geometry::DestroyGeometry(gser2);
		return dis;
	}
};

struct GeometryDistanceTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA geom1, TB geom2, TC use_spheroid) {
		double dis = 0.00;
		if (geom1.GetSize() == 0 || geom2.GetSize() == 0) {
			return dis;
		}
		auto gser1 = Geometry::GetGserialized(geom1);
		auto gser2 = Geometry::GetGserialized(geom2);
		if (!gser1 || !gser2) {
			throw ConversionException("Failure in geometry distance: could not calculate distance from geometries");
		}
		dis = Geometry::Distance(gser1, gser2, use_spheroid);
		Geometry::DestroyGeometry(gser1);
		Geometry::DestroyGeometry(gser2);
		return dis;
	}
};

template <typename TA, typename TB, typename TR>
static void GeometryDistanceBinaryExecutor(Vector &geom1, Vector &geom2, Vector &result, idx_t count) {
	BinaryExecutor::ExecuteStandard<TA, TB, TR, GeometryDistanceBinaryOperator>(geom1, geom2, result, count);
}

template <typename TA, typename TB, typename TC, typename TR>
static void GeometryDistanceTernaryExecutor(Vector &geom1, Vector &geom2, Vector &use_spheroid, Vector &result,
                                            idx_t count) {
	TernaryExecutor::Execute<TA, TB, TC, TR>(geom1, geom2, use_spheroid, result, count,
	                                         GeometryDistanceTernaryOperator::Operation<TA, TB, TC, TR>);
}

void GeoFunctions::GeometryDistanceFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom1_arg = args.data[0];
	auto &geom2_arg = args.data[1];
	if (args.data.size() == 2) {
		GeometryDistanceBinaryExecutor<string_t, string_t, double>(geom1_arg, geom2_arg, result, args.size());
	} else if (args.data.size() == 3) {
		auto &use_spheroid_arg = args.data[2];
		GeometryDistanceTernaryExecutor<string_t, string_t, bool, double>(geom1_arg, geom2_arg, use_spheroid_arg,
		                                                                  result, args.size());
	}
}

struct CentroidUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA geom) {
		return geom;
		// if (geom.GetSize() == 0) {
		// 	return NULL;
		// }
		// auto gser = Geometry::GetGserialized(geom);
		// if (!gser) {
		// 	throw ConversionException("Failure in geometry centroid: could not calculate centroid from geometry");
		// }
		// auto result = Geometry::Centroid(gser);
		// idx_t rv_size = Geometry::GetGeometrySize(result);
		// auto base = Geometry::GetBase(result);
		// Geometry::DestroyGeometry(result);
		// return string_t((const char *)base, rv_size);
	}
};

struct CentroidBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA geom, TB use_spheroid) {
		return geom;
		// if (geom.GetSize() == 0) {
		// 	return NULL;
		// }
		// auto gser = Geometry::GetGserialized(geom);
		// if (!gser) {
		// 	throw ConversionException("Failure in geometry centroid: could not calculate centroid from geometry");
		// }
		// auto result = Geometry::Centroid(gser, use_spheroid);
		// idx_t rv_size = Geometry::GetGeometrySize(result);
		// auto base = Geometry::GetBase(result);
		// Geometry::DestroyGeometry(result);
		// return string_t((const char *)base, rv_size);
	}
};

template <typename TA, typename TR>
static void GeometryCentroidUnaryExecutor(Vector &geom, Vector &result, idx_t count) {
	UnaryExecutor::Execute<TA, TR, CentroidUnaryOperator>(geom, result, count);
}

template <typename TA, typename TB, typename TR>
static void GeometryCentroidBinaryExecutor(Vector &geom, Vector &use_spheroid, Vector &result, idx_t count) {
	BinaryExecutor::ExecuteStandard<TA, TB, TR, CentroidBinaryOperator>(geom, use_spheroid, result, count);
}

void GeoFunctions::GeometryCentroidFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &geom_arg = args.data[0];
	if (args.data.size() == 1) {
		GeometryCentroidUnaryExecutor<string_t, string_t>(geom_arg, result, args.size());
	} else if (args.data.size() == 2) {
		auto &use_spheroid_arg = args.data[1];
		GeometryCentroidBinaryExecutor<string_t, bool, string_t>(geom_arg, use_spheroid_arg, result, args.size());
	}
}

struct FromTextUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text) {
		if (text.GetSize() == 0) {
			return text;
		}
		auto gser = Geometry::FromText(&text.GetString()[0]);
		if (!gser) {
			throw ConversionException("Failure in geometry from text: could not convert text to geometry");
		}
		idx_t size = Geometry::GetGeometrySize(gser);
		auto base = Geometry::GetBase(gser);
		Geometry::DestroyGeometry(gser);
		return string_t((const char *)base, size);
	}
};

struct FromTextBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA text, TB srid) {
		if (text.GetSize() == 0) {
			return text;
		}
		auto gser = Geometry::FromText(&text.GetString()[0], srid);
		if (!gser) {
			throw ConversionException("Failure in geometry from text: could not convert text to geometry");
		}
		idx_t size = Geometry::GetGeometrySize(gser);
		auto base = Geometry::GetBase(gser);
		Geometry::DestroyGeometry(gser);
		return string_t((const char *)base, size);
	}
};

template <typename TA, typename TR>
static void GeometryFromTextUnaryExecutor(Vector &text, Vector &result, idx_t count) {
	UnaryExecutor::Execute<TA, TR, FromTextUnaryOperator>(text, result, count);
}

template <typename TA, typename TB, typename TR>
static void GeometryFromTextBinaryExecutor(Vector &text, Vector &srid, Vector &result, idx_t count) {
	BinaryExecutor::ExecuteStandard<TA, TB, TR, FromTextBinaryOperator>(text, srid, result, count);
}

void GeoFunctions::GeometryFromTextFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
	if (args.data.size() == 1) {
		GeometryFromTextUnaryExecutor<string_t, string_t>(text_arg, result, args.size());
	} else if (args.data.size() == 2) {
		auto &srid_arg = args.data[1];
		GeometryFromTextBinaryExecutor<string_t, int32_t, string_t>(text_arg, srid_arg, result, args.size());
	}
}

struct FromWKBUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text) {
		if (text.GetSize() == 0) {
			return text;
		}
		auto gser = Geometry::FromWKB(text.GetDataUnsafe(), text.GetSize());
		if (!gser) {
			throw ConversionException("Failure in geometry from WKB: could not convert WKB to geometry");
		}
		idx_t size = Geometry::GetGeometrySize(gser);
		auto base = Geometry::GetBase(gser);
		Geometry::DestroyGeometry(gser);
		return string_t((const char *)base, size);
	}
};

struct FromWKBBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA text, TB srid) {
		if (text.GetSize() == 0) {
			return text;
		}
		auto gser = Geometry::FromWKB(text.GetDataUnsafe(), text.GetSize(), srid);
		if (!gser) {
			throw ConversionException("Failure in geometry from WKB: could not convert WKB to geometry");
		}
		idx_t size = Geometry::GetGeometrySize(gser);
		auto base = Geometry::GetBase(gser);
		Geometry::DestroyGeometry(gser);
		return string_t((const char *)base, size);
	}
};

template <typename TA, typename TR>
static void GeometryFromWKBUnaryExecutor(Vector &text, Vector &result, idx_t count) {
	UnaryExecutor::Execute<TA, TR, FromWKBUnaryOperator>(text, result, count);
}

template <typename TA, typename TB, typename TR>
static void GeometryFromWKBBinaryExecutor(Vector &text, Vector &srid, Vector &result, idx_t count) {
	BinaryExecutor::ExecuteStandard<TA, TB, TR, FromWKBBinaryOperator>(text, srid, result, count);
}

void GeoFunctions::GeometryFromWKBFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
	if (args.data.size() == 1) {
		GeometryFromWKBUnaryExecutor<string_t, string_t>(text_arg, result, args.size());
	} else if (args.data.size() == 2) {
		auto &srid_arg = args.data[1];
		GeometryFromWKBBinaryExecutor<string_t, int32_t, string_t>(text_arg, srid_arg, result, args.size());
	}
}

struct GetXUnaryOperator {
	template <class TA, class TR>
	static inline TR Operation(TA text) {
		if (text.GetSize() == 0) {
			// throw ConversionException(
			//     "Failure in geometry get X: could not get coordinate X from geometry");
			return 0.00;
		}
		double x_val = Geometry::XPoint(text.GetDataUnsafe(), text.GetSize());
		return x_val;
	}
};

template <typename TA, typename TR>
static void GeometryGetXUnaryExecutor(Vector &text, Vector &result, idx_t count) {
	UnaryExecutor::Execute<TA, TR, GetXUnaryOperator>(text, result, count);
}

void GeoFunctions::GeometryGetXFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &text_arg = args.data[0];
	GeometryGetXUnaryExecutor<string_t, double>(text_arg, result, args.size());
}

} // namespace duckdb
