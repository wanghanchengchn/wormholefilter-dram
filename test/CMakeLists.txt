find_package(OpenSSL REQUIRED)

#
add_executable(evaluation evaluation.cpp)
target_link_libraries(evaluation
PRIVATE header
    "-fno-strict-aliasing"
    "-lcrypto"
    "-msse4.2"
    "-D__SSE4_2_"
    )
target_compile_options(evaluation PUBLIC "-mavx2" "-O3")