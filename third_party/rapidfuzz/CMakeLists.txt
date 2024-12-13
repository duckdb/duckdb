if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

set(RAPIDFUZZ_FILES
	details/CharSet.hpp
    details/common.hpp
    details/common_impl.hpp
    details/distance.hpp
    details/GrowingHashmap.hpp
    details/intrinsics.hpp
    details/Matrix.hpp
    details/PatternMatchVector.hpp
    details/Range.hpp
    details/simd_avx2.hpp
    details/simd.hpp
    details/simd_sse2.hpp
    details/SplittedSentenceView.hpp
    details/types.hpp
    details/type_traits.hpp
    distance/DamerauLevenshtein.hpp
    distance/DamerauLevenshtein_impl.hpp
    distance/Hamming.hpp
    distance/Hamming_impl.hpp
    distance/Indel.hpp
    distance/Indel_impl.hpp
    distance/Jaro.hpp
    distance/Jaro_impl.hpp
    distance/JaroWinkler.hpp
    distance/JaroWinkler_impl.hpp
    distance/LCSseq.hpp
    distance/LCSseq_impl.hpp
    distance/Levenshtein.hpp
    distance/Levenshtein_impl.hpp
    distance/OSA.hpp
    distance/OSA_impl.hpp
    distance/Postfix.hpp
    distance/Postfix_impl.hpp
    distance/Prefix.hpp
    distance/Prefix_impl.hpp
    distance.hpp
    fuzz_impl.hpp
    fuzz.hpp
    rapidfuzz_all.hpp
)

add_library(duckdb_rapidfuzz STATIC ${RAPIDFUZZ_FILES})

target_include_directories(
  duckdb_rapidfuzz
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(duckdb_rapidfuzz PROPERTIES EXPORT_NAME duckdb_duckdb_rapidfuzz LINKER_LANGUAGE CXX)

install(TARGETS duckdb_rapidfuzz
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_rapidfuzz)
