#include <iostream>
#include "clerk.h"
#include "config.h"

int main() {
    Clerk client;
    client.Init();
    auto start = now();
    int count = 500;
    int tmp = count;
    while (tmp--) {
        client.Put("x", std::to_string(tmp));

        std::string get1 = client.Get("x");
        std::printf("get return :{%s}\r\n", get1.c_str());
    }
    return 0;
}