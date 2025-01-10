cmake_minimum_required(VERSION 3.5...3.29)

project(CoreFunctionsExtension)

include_directories(include)

set(CORE_FUNCTION_FILES
    core_functions_extension.cpp
    function_list.cpp
    lambda_functions.cpp
    aggregate/holistic/approx_top_k.cpp
    aggregate/holistic/quantile.cpp
    aggregate/holistic/reservoir_quantile.cpp
    aggregate/holistic/mad.cpp
    aggregate/holistic/approximate_quantile.cpp
    aggregate/holistic/mode.cpp
    aggregate/distributive/kurtosis.cpp
    aggregate/distributive/string_agg.cpp
    aggregate/distributive/sum.cpp
    aggregate/distributive/arg_min_max.cpp
    aggregate/distributive/approx_count.cpp
    aggregate/distributive/skew.cpp
    aggregate/distributive/bitagg.cpp
    aggregate/distributive/bitstring_agg.cpp
    aggregate/distributive/product.cpp
    aggregate/distributive/bool.cpp
    aggregate/algebraic/corr.cpp
    aggregate/algebraic/stddev.cpp
    aggregate/algebraic/avg.cpp
    aggregate/algebraic/covar.cpp
    aggregate/regression/regr_sxy.cpp
    aggregate/regression/regr_intercept.cpp
    aggregate/regression/regr_count.cpp
    aggregate/regression/regr_r2.cpp
    aggregate/regression/regr_avg.cpp
    aggregate/regression/regr_slope.cpp
    aggregate/regression/regr_sxx_syy.cpp
    aggregate/nested/binned_histogram.cpp
    aggregate/nested/list.cpp
    aggregate/nested/histogram.cpp
    scalar/bit/bitstring.cpp
    scalar/operators/bitwise.cpp
    scalar/array/array_functions.cpp
    scalar/array/array_value.cpp
    scalar/date/current.cpp
    scalar/date/age.cpp
    scalar/date/date_diff.cpp
    scalar/date/date_sub.cpp
    scalar/date/to_interval.cpp
    scalar/date/time_bucket.cpp
    scalar/date/date_trunc.cpp
    scalar/date/epoch.cpp
    scalar/date/date_part.cpp
    scalar/date/make_date.cpp
    scalar/enum/enum_functions.cpp
    scalar/math/numeric.cpp
    scalar/struct/struct_insert.cpp
    scalar/map/map_keys_values.cpp
    scalar/map/map_extract.cpp
    scalar/map/map_from_entries.cpp
    scalar/map/map_entries.cpp
    scalar/map/map.cpp
    scalar/map/map_concat.cpp
    scalar/map/cardinality.cpp
    scalar/list/flatten.cpp
    scalar/list/list_transform.cpp
    scalar/list/range.cpp
    scalar/list/list_value.cpp
    scalar/list/list_filter.cpp
    scalar/list/list_has_any_or_all.cpp
    scalar/list/list_aggregates.cpp
    scalar/list/list_distance.cpp
    scalar/list/array_slice.cpp
    scalar/list/list_sort.cpp
    scalar/list/list_reduce.cpp
    scalar/union/union_extract.cpp
    scalar/union/union_value.cpp
    scalar/union/union_tag.cpp
    scalar/generic/alias.cpp
    scalar/generic/try.cpp
    scalar/generic/typeof.cpp
    scalar/generic/stats.cpp
    scalar/generic/binning.cpp
    scalar/generic/least.cpp
    scalar/generic/hash.cpp
    scalar/generic/can_implicitly_cast.cpp
    scalar/generic/system_functions.cpp
    scalar/generic/current_setting.cpp
    scalar/string/starts_with.cpp
    scalar/string/jaccard.cpp
    scalar/string/levenshtein.cpp
    scalar/string/damerau_levenshtein.cpp
    scalar/string/bar.cpp
    scalar/string/printf.cpp
    scalar/string/replace.cpp
    scalar/string/hamming.cpp
    scalar/string/instr.cpp
    scalar/string/ascii.cpp
    scalar/string/reverse.cpp
    scalar/string/url_encode.cpp
    scalar/string/parse_path.cpp
    scalar/string/left_right.cpp
    scalar/string/to_base.cpp
    scalar/string/pad.cpp
    scalar/string/trim.cpp
    scalar/string/format_bytes.cpp
    scalar/string/hex.cpp
    scalar/string/repeat.cpp
    scalar/string/translate.cpp
    scalar/string/chr.cpp
    scalar/string/unicode.cpp
    scalar/string/jaro_winkler.cpp
    scalar/random/random.cpp
    scalar/random/setseed.cpp
    scalar/blob/base64.cpp
    scalar/blob/encode.cpp
    scalar/debug/vector_type.cpp)

build_static_extension(core_functions ${CORE_FUNCTION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(core_functions ${PARAMETERS} ${CORE_FUNCTION_FILES})
target_link_libraries(core_functions_loadable_extension duckdb_skiplistlib)

install(
  TARGETS core_functions_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
