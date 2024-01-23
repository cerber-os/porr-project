cmake_minimum_required(VERSION 3.26)
project(pydes)

set(CMAKE_CXX_STANDARD 17)

include_directories(pydes PRIVATE /usr/include/python3.11 /usr/include/python3.11)

include_directories(.)
add_library(pydes SHARED
        des.cpp
        des3.cpp
        pydes.cpp
)
