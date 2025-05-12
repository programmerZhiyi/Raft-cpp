#pragma once

#include <string>
#include <vector>
#include <fstream>
#include <mutex>
#include <boost/archive/text_oarchive.hpp>
#include <boost/archive/text_iarchive.hpp>
#include <boost/serialization/vector.hpp>
#include <boost/serialization/access.hpp>
#include <algorithm>
#include <iostream>

template <typename K, typename V>
class Node {
public:
    Node();
    Node(const K key, const V value, int level);
    ~Node();
    K get_key() const; // 获取节点的键
    V get_value() const;
    void set_value(V value);

    Node<K, V> **forward; // forward[i]指向第i层的下一个节点
    int node_level; // 节点的层数
private:
    K key; // 节点的键
    V value; // 节点的值
};

template <typename K, typename V>
class SkipListDump {
public:
    friend class boost::serialization::access;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version) {
        ar & keyDumpVt_;
        ar & valueDumpVt_;
    }
    std::vector<K> keyDumpVt_;
    std::vector<V> valueDumpVt_;
    void insert(const Node<K, V> &node);
};

template <typename K, typename V>
class SkipList {
public:
    SkipList(int);
    ~SkipList();
    int get_random_level();
    Node<K, V> *create_node(const K key, const V value, int level);
    int insert_element(const K key, const V value);
    void display_list();
    bool search_element(K, V &value);
    void delete_element(K key);
    void insert_set_element(K &key, V &value);
    std::string dump_file();
    void load_file(const std::string &dumpStr);
    // 递归删除节点
    void clear(Node<K, V> *node);
    int size();
private:
    void get_key_value_from_string(const std::string &str, std::string *key, std::string *value);
    bool is_valid_string(const std::string &str);

    int _max_level; // 最大层数
    int _skip_list_level; // 当前层数
    Node<K, V> *_header; // 头节点
    std::ofstream _file_writer; // 文件写入流
    std::ifstream _file_reader; // 文件读取流
    int _element_count; // 元素个数
    std::mutex _mtx; // 互斥锁
};

template <typename K, typename V>
Node<K, V>::Node() {}

template <typename K, typename V>
Node<K, V>::Node(const K key, const V value, int level) : key(key), value(value), node_level(level) {
    forward = new Node<K, V>*[level + 1];
    std::fill_n(forward, level + 1, nullptr);
}

template <typename K, typename V>
Node<K, V>::~Node() {
    delete[] forward;
}

template <typename K, typename V>
K Node<K, V>::get_key() const {
    return key;
}

template <typename K, typename V>
V Node<K, V>::get_value() const {
    return value;
}

template <typename K, typename V>
void Node<K, V>::set_value(V value) {
    this->value = value;
}

// 创建一个新的节点
template <typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(const K key, const V value, int level) {
    Node<K, V>* new_node = new Node<K, V>(key, value, level);
    return new_node;
}

// 插入元素， 返回1表示元素已存在，返回0表示插入成功
template <typename K, typename V>
int SkipList<K, V>::insert_element(const K key, const V value) {
    _mtx.lock();
    Node<K, V>* current = _header;

    // 用于存储前驱节点
    // update[i]表示第i层的前驱节点
    Node<K, V>* update[_max_level + 1];
    std::fill_n(update, _max_level + 1, nullptr);

    // 从最高层开始向下查找
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    current = current->forward[0];
    // 如果当前节点的键等于要插入的键，则插入失败
    if (current != nullptr && current->get_key() == key) {
        std::cout << "key: " << key << ", exists" << std::endl;
        _mtx.unlock();
        return 1;
    }

    // 如果当前节点的键不等于要插入的键，则插入新节点
    // 如果当前节点为null，表示要插入的键大于所有节点的键
    if (current == nullptr || current->get_key() != key) {
        int random_level = get_random_level();
        if (random_level > _skip_list_level) {
            for (int i = _skip_list_level + 1; i <= random_level; i++) {
                update[i] = _header;
            }
            _skip_list_level = random_level;
        }
        Node<K, V>* new_node = create_node(key, value, random_level);
        for (int i = 0; i <= random_level; i++) {
            new_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = new_node;
        }
        std::cout << "Inserted key: " << key << ", value: " << value << std::endl;
        _element_count++;
    }
    _mtx.unlock();
    return 0;
}

// 显示跳表
template <typename K, typename V>
void SkipList<K, V>::display_list() {
    std::cout << "Skip List:" << std::endl;
    for (int i = _skip_list_level; i >= 0; i--) {
        Node<K, V>* current = _header->forward[i];
        std::cout << "Level " << i << ": ";
        while (current != nullptr) {
            std::cout << current->get_key() << ":" << current->get_value() << ";";
            current = current->forward[i];
        }
        std::cout << std::endl;
    }
}

// 序列化跳表
template <typename K, typename V>
std::string SkipList<K, V>::dump_file() { 
    Node<K, V>* current = _header->forward[0];
    SkipListDump<K, V> dumper;
    while (current != nullptr) {
        dumper.insert(*current);
        current = current->forward[0];
    }
    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << dumper;
    return ss.str();
}

// 反序列化跳表
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr) { 
    if (dumpStr.empty()) {
        return;
    }
    SkipListDump<K, V> dumper;
    std::stringstream ss(dumpStr);
    boost::archive::text_iarchive ia(ss);
    ia >> dumper;
    for (size_t i = 0; i < dumper.keyDumpVt_.size(); ++i) {
        insert_element(dumper.keyDumpVt_[i], dumper.valueDumpVt_[i]);
    }
}

// 获取当前跳表的大小
template <typename K, typename V>
int SkipList<K, V>::size() {
    return _element_count;
}


template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string &str, std::string *key, std::string *value) {
    if (!is_valid_string(str)) {
        return;
    }
    *key = str.substr(0, str.find(':'));
    *value = str.substr(str.find(':') + 1, str.length());
}

template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string &str) {
    if (str.empty()) {
        return false;
    }
    if (str.find(':') == std::string::npos) {
        return false;
    }
    return true;
}

// 删除元素
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key) {
    _mtx.lock();
    Node<K, V>* current = _header;
    Node<K, V>* update[_max_level + 1];
    std::fill_n(update, _max_level + 1, nullptr);

    // 从最高层开始向下查找
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
        update[i] = current;
    }
    current = current->forward[0];

    // 如果当前节点的键等于要删除的键，则删除节点
    if (current != nullptr && current->get_key() == key) {
        for (int i = 0; i <= _skip_list_level; i++) {
            if (update[i]->forward[i] != current) {
                break;
            }
            update[i]->forward[i] = current->forward[i];
        }
        delete current;
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == nullptr) {
            _skip_list_level--;
        }
        std::cout << "Deleted key: " << key << std::endl;
        _element_count--;
    }
    _mtx.unlock();
}

template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &value) {
    V oldValue;
    if (search_element(key, oldValue)) {
        delete_element(key);
    }
    insert_element(key, value);
}

template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value) {
    std::cout << "search_element------------------------" << std::endl;
    Node<K, V>* current = _header;

    // 从最高层开始向下查找
    for (int i = _skip_list_level; i >= 0; i--) {
        while (current->forward[i] != nullptr && current->forward[i]->get_key() < key) {
            current = current->forward[i];
        }
    }
    current = current->forward[0];
    // 如果当前节点的键等于要查找的键，则返回true
    if (current && current->get_key() == key) {
        value = current->get_value();
        std::cout << "key: " << key << ", value: " << value << std::endl;
        return true;
    }

    std::cout << "Not Found Key:" << key << std::endl;
    return false;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node) {
    keyDumpVt_.emplace_back(node.get_key());
    valueDumpVt_.emplace_back(node.get_value());
}

// 构建跳表
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level) : _max_level(max_level), _skip_list_level(0), _element_count(0) {
    _header = new Node<K, V>(K(), V(), max_level);
}

template <typename K, typename V>
SkipList<K, V>::~SkipList() {
    clear(_header);
    //delete _header;
}

template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V> *node) {
    if (node->forward[0] != nullptr) {
        clear(node->forward[0]);
    }
    delete node;
}

template <typename K, typename V>
int SkipList<K, V>::get_random_level() {
    int level = 0;
    while (rand() % 2 == 0 && level < _max_level) {
        level++;
    }
    return level;
}