if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

include_directories(include)

set(CMAKE_CXX_VISIBILITY_PRESET hidden)

add_library(duckdb_mbedtls STATIC mbedtls_wrapper.cpp library/sha256.c library/rsa.c library/rsa_alt_helpers.c  library/md.c library/bignum.c library/oid.c library/constant_time.c library/platform_util.c library/base64.c library/pkparse.c library/pk.c library/pk_wrap.c library/asn1parse.c library/pem.c)

set_property(TARGET duckdb_mbedtls PROPERTY C_STANDARD 99)


install(TARGETS duckdb_mbedtls
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
