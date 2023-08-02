#
# This config file holds extensions that are compatible with the revamped vcpkg-based build process
# all extensions from .github/config/out_of_tree_extensions.cmake should be moved here when they are compatible.
#
# TODO Rename after all are moved over
#
# to build duckdb with this configuration run:
#   EXTENSION_CONFIGS=.github/config/vcpkg_extensions.cmake make
#

################# ARROW
if (NOT WIN32)
    duckdb_extension_load(arrow
        DONT_LINK
        GIT_URL https://github.com/duckdblabs/arrow
        GIT_TAG 56d24d4224df43cac0e071d71371385de5831129
    )
endif()

################# AWS
duckdb_extension_load(aws
    LOAD_TESTS
    GIT_URL https://github.com/duckdblabs/duckdb_aws
    GIT_TAG 617a4b1456eec1dee3d668f9ce005a1de9ef21c8
)

################# AZURE
duckdb_extension_load(azure
    LOAD_TESTS
    GIT_URL https://github.com/duckdblabs/duckdb_azure
    GIT_TAG 7cd5149ee879f3ea9e0a9215e0739643dd75eb6e
)

################# ICEBERG
# Windows tests for iceberg currently not working
if (NOT WIN32)
    set(LOAD_ICEBERG_TESTS "LOAD_TESTS")
else ()
    set(LOAD_ICEBERG_TESTS "")
endif()

duckdb_extension_load(iceberg
    ${LOAD_ICEBERG_TESTS}
    GIT_URL https://github.com/duckdblabs/duckdb_iceberg
    GIT_TAG 6481aa4dd0ab9d724a8df28a1db66800561dd5f9
    APPLY_PATCHES
)

################# POSTGRES_SCANNER
duckdb_extension_load(postgres_scanner
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/postgres_scanner
    GIT_TAG cd043b49cdc9e0d3752535b8333c9433e1007a48
)

################# SPATIAL
duckdb_extension_load(spatial
    DONT_LINK
    GIT_URL https://github.com/duckdblabs/duckdb_spatial.git
    GIT_TAG dc66594776fbe2f0a8a3af30af7f9f8626e6e215
    INCLUDE_DIR spatial/include
    TEST_DIR test/sql
)

################# SQLITE_SCANNER
# Static linking on windows does not properly work due to symbol collision
if (WIN32)
    set(STATIC_LINK_SQLITE "DONT_LINK")
else ()
    set(STATIC_LINK_SQLITE "")
endif()

duckdb_extension_load(sqlite_scanner
        ${STATIC_LINK_SQLITE}
        LOAD_TESTS
        GIT_URL https://github.com/duckdblabs/sqlite_scanner
        GIT_TAG 9c38a30be2237456cdcd423d527b96c944158c77
        )

################# SUBSTRAIT
if (NOT WIN32)
    duckdb_extension_load(substrait
            DONT_LINK
            GIT_URL https://github.com/duckdblabs/substrait
            GIT_TAG 53da781310c9c680efb97576d33a5fde89a58870
            )
endif()