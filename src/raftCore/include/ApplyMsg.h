#pragma once

#include <string>
class ApplyMsg {
public:
    bool CommandValid;
    std::string Command;
    int CommandIndex;
    bool SnapshotValid;
    std::string Snapshot;
    int SnapshotTerm;
    int SnapshotIndex;
    //两个valid最开始要赋予false！！
    ApplyMsg()
        : CommandValid(false),
            Command(),
            CommandIndex(-1),
            SnapshotValid(false),
            SnapshotTerm(-1),
            SnapshotIndex(-1){
    }
};