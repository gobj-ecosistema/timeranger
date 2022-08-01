##############################################
#   CMake
##############################################
project(time2date C)
cmake_minimum_required(VERSION 3.13)
include(CheckIncludeFiles)
include(CheckSymbolExists)

set(CMAKE_INSTALL_PREFIX /yuneta/development/output)

set(INC_DEST_DIR ${CMAKE_INSTALL_PREFIX}/include)
set(LIB_DEST_DIR ${CMAKE_INSTALL_PREFIX}/lib)
set(BIN_DEST_DIR /yuneta/bin)

set(CMAKE_C_STANDARD 99)

if(LINUX)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
endif()

if(CMAKE_BUILD_TYPE MATCHES Debug)
  add_definitions(-DDEBUG)
  option(SHOWNOTES "Show preprocessor notes" OFF)

  if(CMAKE_COMPILER_IS_GNUCC)
    # GCC specific debug options
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O0 -g3 -ggdb3 -gdwarf-2")
    set(AVOID_VERSION -avoid-version)
  endif(CMAKE_COMPILER_IS_GNUCC)
endif(CMAKE_BUILD_TYPE MATCHES Debug)

add_definitions(-D_GNU_SOURCE)
add_definitions(-D_LARGEFILE_SOURCE -D_FILE_OFFSET_BITS=64)

IF(WIN32)
    include_directories(c:/yuneta/development/output/include)
    include_directories(c:/yuneta/development/output/include/regex)
    include_directories(c:/yuneta/development/output/OpenSSL-Win64/include)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
ELSEIF(UNIX)
    include_directories(/yuneta/development/output/include)
ENDIF()

##############################################
#   Source
##############################################

SET (YUNO_SRCS
    time2date.c
)

##############################################
#   yuno
##############################################
IF(WIN32)
    LINK_DIRECTORIES(c:/yuneta/development/output/lib/)
ELSEIF(UNIX)
    LINK_DIRECTORIES(/yuneta/development/output/lib/)
ENDIF()

ADD_EXECUTABLE(time2date ${YUNO_SRCS} ${YUNO_HDRS})

IF(WIN32)
    TARGET_LINK_LIBRARIES(time2date
        ghelpers
        uv
        jansson_d
        pcre2-8-staticd
        regex
        argp
        ws2_32
    )
ELSEIF(UNIX)
    TARGET_LINK_LIBRARIES(time2date
        libghelpers.a
        libuv.a
        libjansson.a
        libunwind.a
        libpcre2-8.a

        pthread dl  # used by libuv
        lzma        # used by libunwind
        m
        util
    )
ENDIF()


##############################################
#   Installation
##############################################
install(
    TARGETS time2date
    PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_WRITE GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
    DESTINATION ${BIN_DEST_DIR}
)
