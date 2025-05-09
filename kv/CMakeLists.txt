set(SRC_LIST kvserver.cc kv.pb.cc)
set(SRC_LIST1 kvclient.cc kv.pb.cc)

add_executable(server ${SRC_LIST})
add_executable(client ${SRC_LIST1})
target_link_libraries(server ${LIBS})
target_link_libraries(client ${LIBS})