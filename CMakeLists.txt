cmake_minimum_required(VERSION 3.21)
project(debug)

set(CMAKE_CXX_STANDARD 14)

add_executable(debug main.cpp kvstore.cc skiplist.cpp)
