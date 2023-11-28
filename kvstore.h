#pragma once

#include "kvstore_api.h"
#include <bitset>
#include "skiplist.h"
#include "utils.h"

struct sst_buf {
    uint64_t time; // SSTable的时间戳
    uint64_t num; // SSTable键值对的数量
    uint64_t min; // 键的最小值
    uint64_t max; // 键的最大值

    // Bloom Filter
    std::bitset<10240 * 8> arr;

    // 索引区，动态数组大小为键值对数量 num 的两倍
    uint64_t *key;
    uint32_t *offset;

    // 读入内存的SSTable形成单链表
    sst_buf *next;

    // 在缓冲区的每个结构体中加入磁盘对应文件的路径 (COMPACTION)
    std::string path;

    sst_buf() {
        time = 0;
        num = 0;
        min = 0;
        max = 0;
        key = nullptr;
        offset = nullptr;
        next = nullptr;
    };
};

class KVStore : public KVStoreAPI {
private:

    sst_buf *head;
    uint64_t time_max;
    std::string file;

    void WriteToDisk();
    static uint64_t binarySearch(const uint64_t a[], uint64_t n, uint64_t target);
    static void update_list(uint64_t key, const std::string val, uint64_t key1, uint64_t key2, std::list< std::pair<uint64_t, std::string> > &list);

    // COMPACTION
    int level_max;
    int IsLevelFull(const std::string &dir);
    void compaction(); // 默认是从 level0 到 level1 的 compaction
    void compaction(int level); // 默认是从 level 到 level + 1 的 compaction
    sst_buf* GetBuffer(const std::string &path);
    void split(std::list<std::pair<uint64_t, std::string> > &list, int level, uint64_t timeStamp);
    static uint64_t cal_split(std::list<std::pair<uint64_t, std::string> > &list);
    void expansion(std::list<std::pair<uint64_t, std::string> > &list, uint64_t &timeStamp, int cur_level, uint64_t time_record[]);
    std::vector<std::string> split(char c, std::string src);

public:

    SkipList MemTable;

	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

    static std::string GetString(const std::string &path, uint64_t key);
};
