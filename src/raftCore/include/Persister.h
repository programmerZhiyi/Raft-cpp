#pragma once

#include <mutex>
#include <fstream>

class Persister {
private:
    std::mutex m_mtx;
    std::string m_raftState;
    std::string m_snapshot;
    const std::string m_raftStateFileName; // raftState文件名
    const std::string m_snapshotFileName; // snapshot文件名
    std::ofstream m_raftStateOutStream; // raftState文件输出流
    std::ofstream m_snapshotOutStream; // snapshot文件输出流
    long long m_raftStateSize; // raftState文件大小
    void clearRaftState();
    void clearSnapshot();
    void clearRaftStateAndSnapshot();
public:
    void Save(std::string raftstate, std::string snapshot);
    std::string ReadSnapshot();
    void SaveRaftState(const std::string &data);
    long long RaftStateSize();
    std::string ReadRaftState();
    explicit Persister(int me);
    ~Persister();
};