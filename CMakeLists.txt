##############################################
#   CMake
##############################################
cmake_minimum_required(VERSION 3.11)
project(timeranger C)
include(CheckIncludeFiles)
include(CheckSymbolExists)

set(CMAKE_INSTALL_PREFIX /yuneta/development/output)

set(INC_DEST_DIR ${CMAKE_INSTALL_PREFIX}/include)
set(LIB_DEST_DIR ${CMAKE_INSTALL_PREFIX}/lib)
set(BIN_DEST_DIR ${CMAKE_INSTALL_PREFIX}/bin)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -std=c99")


##############################################
#   Source
##############################################
# add_subdirectory(tranger_create)
add_subdirectory(time2date)
add_subdirectory(tranger_list)
add_subdirectory(tranger_search)
add_subdirectory(trq_list)
add_subdirectory(trmsg_list)
add_subdirectory(msg2db_list)
add_subdirectory(treedb_list)
add_subdirectory(tranger_migrate)
add_subdirectory(time2range)
add_subdirectory(json_diff)
add_subdirectory(tranger_delete)
