# 设置cmake的最低版本和项目名称
cmake_minimum_required(VERSION 3.0)
project(Raft)

# 生成debug版本，可以进行gdb调试
set(CMAKE_BUILD_TYPE "Debug")

# 设置项目可执行文件输出的路径
set(EXECUTABLE_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/bin)
# 设置项目库文件输出的路径
set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)

# 设置项目编译头文件搜索路径 -I
#include_directories(${PROJECT_SOURCE_DIR}/lib/include)
include_directories(${PROJECT_SOURCE_DIR}/src/skipList/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftCore/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpcService/include)
include_directories(${PROJECT_SOURCE_DIR}/src/common/include)
include_directories(${PROJECT_SOURCE_DIR}/src/rpcSrc/include)
include_directories(${PROJECT_SOURCE_DIR}/src/raftClerk/include)
# 设置项目库文件搜索路径 -L
link_directories(${PROJECT_SOURCE_DIR}/lib)

set(LIBS mprpc protobuf pthread zookeeper_mt muduo_net muduo_base glog boost_serialization)

add_subdirectory(src)
add_subdirectory(example)

add_library(skip_list_on_raft STATIC  ${src_rpcsrc} ${src_raftCore} ${src_rpcservice})
target_link_libraries(skip_list_on_raft muduo_net muduo_base pthread dl)

# add_custom_target(format
#         COMMAND bash ${PROJECT_SOURCE_DIR}/format.sh
#         WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
#         COMMAND echo "format done!"
# )