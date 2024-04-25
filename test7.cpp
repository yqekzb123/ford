#include <tbb/concurrent_hash_map.h>
#include <iostream>

typedef tbb::concurrent_hash_map<int, std::string> HashTable;

int main() {
    HashTable hashTable;

    // 插入键值对
    hashTable.insert(std::make_pair(1, "Value1"));
    hashTable.insert(std::make_pair(2, "Value2"));
    hashTable.insert(std::make_pair(3, "Value3"));

    // 查找键值对
    HashTable::const_accessor accessor;
    if (hashTable.find(accessor, 2)) {
        std::cout << "Value found: " << accessor->second << std::endl;
    } else {
        std::cout << "Value not found" << std::endl;
    }

    // 删除键值对
    hashTable.erase(1);

    return 0;
}