#pragma once

#include <vector>
#include <climits>
#include <ctime>
#include <cstdlib>
#include "kvstore_api.h"
#include <iostream>

#define MAX_LEVEL 8

enum SKNodeType
{
    HEAD = 1,
    NORMAL,
    NIL
};

struct SKNode
{
    uint64_t key;
    std::string val;
    SKNodeType type;
    std::vector<SKNode *> forwards;
    SKNode(uint64_t _key, std::string _val, SKNodeType _type)
            : key(_key), val(_val), type(_type)
    {
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            forwards.push_back(nullptr);
        }
    }
};

class SkipList
{
private:
    unsigned long long s = 1;
    double my_rand();
    int randomLevel();
    int dataLength = 10272;  // 当前跳表转化为 sst 文件的基础长度

public:
    SKNode *head;
    SKNode *NIL;
    SkipList()
    {
        head = new SKNode(0, "", SKNodeType::HEAD);
        NIL = new SKNode(ULONG_LONG_MAX, "", SKNodeType::NIL);
        for (int i = 0; i < MAX_LEVEL; ++i)
        {
            head->forwards[i] = NIL;
        }
    }
    void Insert(uint64_t key, std::string value);
    bool Search(uint64_t key, std::string &str_ptr) const;
    void Display();
    int GetCurrentDataLength() const {return dataLength;}
    void CleanDataLength() {dataLength = 10272;}
    ~SkipList()
    {
        SKNode *n1 = head;
        SKNode *n2;
        while (n1)
        {
            n2 = n1->forwards[0];
            delete n1;
            n1 = n2;
        }
    }
};
