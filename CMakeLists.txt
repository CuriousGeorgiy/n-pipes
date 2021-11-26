cmake_minimum_required(VERSION 3.16.2)
project(n-pipes C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_definitions(_GNU_SOURCE __USE_GNU)
add_link_options(-lm)

add_executable(n-pipes main.c)
