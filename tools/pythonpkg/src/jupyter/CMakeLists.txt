# this is used for clang-tidy checks
add_library(python_jupyter OBJECT jupyter_progress_bar_display.cpp)

target_link_libraries(python_jupyter PRIVATE pytooling)
