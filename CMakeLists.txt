cmake_minimum_required(VERSION 3.14)
project(transactions_cxx)

set(CMAKE_CXX_STANDARD 17)

add_library(transactions_cxx SHARED library.cpp library.h)
