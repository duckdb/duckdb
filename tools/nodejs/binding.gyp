{
    "targets": [
        {
            "target_name": "<(module_name)",
            "sources": [
                 "src/node_duckdb.cc",
                "src/database.cc",
                "src/statement.cc",
                "src/duckdb.cpp" # comment this out to build against existing lib
            ],
            "include_dirs": [
                "<!@(node -p \"require('node-addon-api').include\")"
            ],
            "cflags_cc": [
                "-frtti"
            ],
            "cflags_cc!": [
                "-fno-exceptions",
                "-fno-rrti"
            ],
            "xcode_settings": {
                "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
                "GCC_ENABLE_CPP_RTTI": "YES",
                "CLANG_CXX_LIBRARY": "libc++",
                "MACOSX_DEPLOYMENT_TARGET": "10.7"
            },
            "msvs_settings": {
                "VCCLCompilerTool": {
                    "ExceptionHandling": 1
                }
            },
            # "libraries": [
            #   "/Users/hannes/source/duckdb/build/release/src/libduckdb_static.a",
            #   "/Users/hannes/source/duckdb/build/release/third_party/fmt/libfmt.a",
            #   "/Users/hannes/source/duckdb/build/release/third_party/libpg_query/libpg_query.a",
            #   "/Users/hannes/source/duckdb/build/release/third_party/utf8proc/libutf8proc.a"
            #   ]
        },
        {
      "target_name": "action_after_build",
      "type": "none",
        "dependencies": [
          "<!(node -p \"require('node-addon-api').gyp\")"
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
