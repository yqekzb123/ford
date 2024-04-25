/*
 * @Description: 
 */
#pragma once

// #include "common/config.h"
// #include "common/rwlatch.h"
#include <string>
#include <cstring>
#include "common.h"

struct Rid {
    page_id_t page_no_;
    int slot_no_;

    friend bool operator==(const Rid &x, const Rid &y) {
        return x.page_no_ == y.page_no_ && x.slot_no_ == y.slot_no_;
    }

    friend bool operator!=(const Rid &x, const Rid &y) { return !(x == y); }
} Aligned8;


/**
 * @description: 存储层每个Page的id的声明
 */
struct PageId {
    // int fd;  //  Page所在的磁盘文件开启后的文件描述符, 来定位打开的文件在内存中的位置
    table_id_t table_id = INVALID_TABLE_ID;
    page_id_t page_no = INVALID_PAGE_ID;

    PageId() = default;
    PageId(int table_id, page_id_t page_no) : table_id(table_id), page_no(page_no) {}

    friend bool operator==(const PageId &x, const PageId &y) { return x.table_id == y.table_id && x.page_no == y.page_no; }
    bool operator<(const PageId& x) const {
        if(table_id < x.table_id) return true;
        return page_no < x.page_no;
    }

    std::string toString() {
        return "{table_id: " + std::to_string(table_id) + " page_no: " + std::to_string(page_no) + "}"; 
    }

    inline int64_t Get() const {
        return (static_cast<int64_t>(table_id << 16) | page_no);
    }
};

// 模板特化, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
namespace std {
template <>
struct hash<PageId> {
    size_t operator()(const PageId &x) const { return (x.table_id << 16) | x.page_no; }
};
}  // namespace std

// PageId的自定义哈希算法, 用于构建unordered_map<PageId, frame_id_t, PageIdHash>
struct PageIdHash {
    size_t operator()(const PageId &x) const { return (x.table_id << 16) | x.page_no; }
    static size_t hash( const PageId& x ) {
        return (x.table_id << 16) | x.page_no;
    }
    //! True if strings are equal
    static bool equal( const PageId& x, const PageId& y ) {
        return x.page_no == y.page_no && x.table_id == y.table_id;
    }
};

/**
 * @description: Page类声明, Page是rucbase数据块的单位、是负责数据操作Record模块的操作对象，
 * Page对象在磁盘上有文件存储, 若在Buffer中则有帧偏移, 并非特指Buffer或Disk上的数据
 */
class MemPage {

   public:
    
    MemPage() { reset_memory(); }

    ~MemPage() = default;

    inline char *get_data() { return data_; }

   private:
    void reset_memory() { memset(data_, 0, PAGE_SIZE); }  // 将data_的PAGE_SIZE个字节填充为0

    /** The actual data that is stored within a page.
     *  该页面在bufferPool中的偏移地址
     */
    char data_[PAGE_SIZE] = {};
};