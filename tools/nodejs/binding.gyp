{
    "targets": [
        {
            "target_name": "<(module_name)",
            "sources": [
                "src/duckdb_node.cpp",
                "src/database.cpp",
                "src/data_chunk.cpp",
                "src/connection.cpp",
                "src/statement.cpp",
                "src/utils.cpp",
                "src/parquet-amalgamation.cpp",
                "src/duckdb.cpp" # comment this out to build against existing lib
            ],
            "include_dirs": [
                "<!@(node -p \"require('node-addon-api').include\")"
            ],
            'defines': [
                'NAPI_DISABLE_CPP_EXCEPTIONS=1',
                "NAPI_VERSION=5"
            ],
            "cflags_cc": [
                "-frtti",
                "-fexceptions",
                "-Wno-redundant-move",
            ],
            "cflags_cc!": [
                "-fno-rrti",
                "-fno-exceptions",
            ],
            "cflags": [
                "-frtti",
                "-fexceptions",
                "-Wno-redundant-move",
            ],
            "cflags!": [
                "-fno-rrti",
                "-fno-exceptions",
            ],
            "xcode_settings": {
                "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
                "GCC_ENABLE_CPP_RTTI": "YES",
                "CLANG_CXX_LIBRARY": "libc++",
                "MACOSX_DEPLOYMENT_TARGET": "10.15",
                'CLANG_CXX_LANGUAGE_STANDARD':'c++11',
                'OTHER_CFLAGS' : ['-fexceptions', '-frtti', '-Wno-redundant-move']

            },
            "msvs_settings": {
                "VCCLCompilerTool": {
                    "ExceptionHandling": 1,
                    "AdditionalOptions": [
                        "/bigobj"
                    ]
                }
            },
            "conditions": [
                [
                    'OS=="win"',
                    {
                        "defines": [
                            "DUCKDB_BUILD_LIBRARY"
                        ]
                    },
                ],  # OS=="win"
            ],  # conditions
          #  uncomment this to build against existing lib
#             "libraries": [
#                 "/Users/hannes/source/duckdb/build/release/src/libduckdb_static.a",
#                 "/Users/hannes/source/duckdb/build/release/third_party/fmt/libduckdb_fmt.a",
#                 "/Users/hannes/source/duckdb/build/release/third_party/libpg_query/libduckdb_pg_query.a",
#                 "/Users/hannes/source/duckdb/build/release/third_party/utf8proc/libduckdb_utf8proc.a",
#                 "/Users/hannes/source/duckdb/build/release/third_party/re2/libduckdb_re2.a"
#             ]
        },
        {
            "target_name": "action_after_build",
            "type": "none",
            "dependencies": [
                "<(module_name)"
            ],
            "copies": [
                {
                    "files": [ "<(PRODUCT_DIR)/<(module_name).node" ],
                    "destination": "<(module_path)"
                }
            ]
        }
    ]
}
