# 获取当前源文件所在目录的绝对路径
get_filename_component(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}" ABSOLUTE)

# 将该目录下所有源文件添加到 SRC_LIST 变量中
aux_source_directory(${SRC_DIR} SRC_LIST)
# 将 raftCore 目录下收集到的源文件列表（SRC_LIST）存储到 src_raftCore 变量中，并使其在 CMake 整个构建系统中可见
set(src_raftCore ${SRC_LIST} CACHE INTERNAL "Description of the variable")