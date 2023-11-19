cmake_minimum_required(VERSION 3.5)
project(protobuf_parser)

set(CMAKE_CXX_STANDARD 20)
include_directories(${CMAKE_CURRENT_BINARY_DIR})

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS proto_files/message.proto)
add_library(proto ${PROTO_SRCS} ${PROTO_HDRS})
