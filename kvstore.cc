#include "kvstore.h"
#include <string>
#include <fstream>
#include "MurmurHash3.h"
#include <cstdio>
#include "string.h"

KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{
    file = dir;
    time_max = 0;
    level_max = -1; // -1 说明目前尚未有任何目录存在

    head = new sst_buf;
    head->next = nullptr; // 首先构造头结点

    // 从现有的全部SSTable中读入sst_buf至内存 (COMPACTION 注意要分层地读)
    std::ifstream in;

    bool isEmpty; // 首先判断 level0 是否存在
    isEmpty = utils::dirExists(file + "/level0");

    while (true) { // 外层循环对应着对于文件夹的遍历

        if (!isEmpty) {
            break;
        } else {
            level_max ++ ;
        }

        std::vector<std::string> all_files; // 获取当前目录所有的文件名放入 vector 之中
        // 利用内置 scanDir 函数得到所有的文件名
        std::string dirname = file + "/level" + std::to_string(level_max);
        utils::scanDir(dirname, all_files);

        std::vector<std::string>::iterator Iter;
        Iter = all_files.begin();

        while (Iter != all_files.end()) { // 内层循环需要遍历一层中的所有文件
            std::string level_no = std::to_string(level_max);
            std::string filename = file + "/level" + level_no + "/" + *Iter;
            in.open(filename, std::ios::binary|std::ios::in);

            auto *add = new sst_buf;

            add->path = filename; // for compaction

            in.read((char*)&(add->time), sizeof(uint64_t));
            in.read((char*)&(add->num), sizeof(uint64_t));
            in.read((char*)&(add->min), sizeof(uint64_t));
            in.read((char*)&(add->max), sizeof(uint64_t));

            in.read((char*)&(add->arr), sizeof(add->arr)); // 载入 Bloom Filter

            add->key = new uint64_t [add->num];
            add->offset = new uint32_t [add->num]; // 动态分配内存

            for (int i=0; i<add->num; ++i) {
                in.read((char*)&(add->key[i]), sizeof(uint64_t));
                in.read((char*)&(add->offset[i]), sizeof(uint32_t));
            }

            in.close();

            // 将创建的结构体插入到链表的头部
            add->next = head->next;
            head->next = add;

            // 确定最大时间戳
            if (add->time > time_max)
                time_max = add->time;

            Iter ++ ; // 开始读取下一个文件
        }

        // 读完本层的所有文件，开始判断下一层文件夹是否存在
        std::string next_level = std::to_string(level_max + 1);
        std::string next_dir = file + "/level" + next_level;
        isEmpty = utils::dirExists(next_dir);
    }
}

KVStore::~KVStore()
{
    // 将目前内存内容全部写入磁盘 SSTable
    WriteToDisk();

    // 将链表以及跳表（自动）进行析构
    sst_buf *del = head;
    while (head != nullptr) {
        head = head->next;
        delete [] del->key;
        delete [] del->offset;
        delete del;
        del = head;
    }
}

void KVStore::WriteToDisk() {

    utils::mkdir((file + "/level0").c_str());

    SKNode *travel = MemTable.head->forwards[0];
    if (travel->type == SKNodeType::NIL)
        return; // 说明这是一个空跳表，直接返回

    // 将跳表的底层转换为 SSTable
    auto *record = new sst_buf;
    time_max++;
    record->time = time_max;
    record->num = 0;

    while (true) {
        if (record->num == 0)
            record->min = travel->key;

        record->num++;

        if (travel->forwards[0]->type == SKNodeType::NIL)
            record->max = travel->key;

        travel = travel->forwards[0];

        if (travel->type == SKNodeType::NIL)
            break;
    } // 可以首先获得键值对的数量，以及最大最小值

    record->key = new uint64_t [record->num];
    record->offset = new uint32_t [record->num]; // 动态分配内存

    // 获得所有的键与值
    int *str_len = new int [record->num];
    travel = MemTable.head->forwards[0];
    int i=0;
    while (true) {
        record->key[i] = travel->key;
        str_len[i] = (int)(travel->val).length();

        travel = travel->forwards[0];
        i++;

        if (travel->type == SKNodeType::NIL)
            break;
    }

    uint32_t offset = 32 + 10240 + 12 * record->num; // 数据区的起始地址
    // 获得字符串长度数组，并且设置所有的 offset 取值
    for (int j=0; j<record->num; ++j) {
        record->offset[j] = offset;
        offset += str_len[j];
    }
    delete [] str_len;

    // 引入哈希函数设置 Bloom Filter
    for (int j=0; j < record->num; ++j) {
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(record->key + j, sizeof(uint64_t), 1, hash);
        record->arr[hash[0] % 81920] = true;
        record->arr[hash[1] % 81920] = true;
        record->arr[hash[2] % 81920] = true;
        record->arr[hash[3] % 81920] = true;
    }

    // 将 SSTable 写入 .sst 文件，并设置 offset
    std::string num = std::to_string(record->time-1);
    std::ofstream out(file + "/level0/level0_" + num + ".sst", std::ios::out|std::ios::app|std::ios::binary);

    record->path = file + "/level0/level0_" + num + ".sst";

    out.write((char*)&record->time, sizeof(uint64_t));
    out.write((char*)&record->num, sizeof(uint64_t));
    out.write((char*)&record->min, sizeof(uint64_t));
    out.write((char*)&record->max, sizeof(uint64_t));

    out.write((char*)&record->arr, sizeof(record->arr));

    for (int j=0; j<record->num; ++j) {
        out.write((char*)&record->key[j], sizeof(uint64_t));
        out.write((char*)&record->offset[j], sizeof(uint32_t));
    }

    travel = MemTable.head->forwards[0];
    while (true) {
        out.write((travel->val).c_str(), (int)(travel->val).length());

        travel = travel->forwards[0];
        if (travel->type == SKNodeType::NIL)
            break;
    }

    out.close();

    // 将磁盘中该文件记入缓存
    record->next = head->next;
    head->next = record;

    // 将跳表的内容清零
    MemTable.CleanDataLength();
    SKNode *del = MemTable.head->forwards[0];
    while (del->type != SKNodeType::NIL) {
        SKNode *tmp = del;
        del = del->forwards[0];
        delete tmp;
    }

    for (int j = 0; j < MAX_LEVEL; ++j)
    {
        MemTable.head->forwards[j] = del;
    }

    // 先将所有的文件放入 level0, 接下来再调用 compaction handler 进行分层归并处理
    bool flag = IsLevelFull("level0");
    if (flag)
        compaction();
}

// 函数参数 path: 磁盘上文件的路径名
// 函数返回值 sst_buf* : 磁盘文件对应缓冲区结构体的指针
// 函数功能：将目标结构体从链表中取下来
sst_buf* KVStore::GetBuffer(const std::string &path) {
    sst_buf* search = head->next;
    sst_buf* del = head;
    while (search->path != path) {
        del = search;
        search = del->next;
    }
    del->next = search->next;
    search->next = nullptr;
    return search;
}

// 函数参数 path: 磁盘上文件的路径名
// 函数参数 key; 函数返回值 string: 由某一缓冲区结构体键值，得到对应磁盘文件数据区字符串值
std::string KVStore::GetString(const std::string &path, uint64_t key) {

    std::ifstream in;
    in.open(path, std::ios::binary|std::ios::in);

    auto *add = new sst_buf;

    in.read((char*)&(add->time), sizeof(uint64_t));
    in.read((char*)&(add->num), sizeof(uint64_t));
    in.read((char*)&(add->min), sizeof(uint64_t));
    in.read((char*)&(add->max), sizeof(uint64_t));

    in.read((char*)&(add->arr), sizeof(add->arr));

    add->key = new uint64_t [add->num];
    add->offset = new uint32_t [add->num]; // 动态分配内存后记得释放堆空间

    uint64_t dest_index = -1; // 确定目标键值的脚标，-1 代表没有找到目标键值，返回空字符串
    for (int i=0; i<add->num; ++i) {
        in.read((char*)&(add->key[i]), sizeof(uint64_t));
        in.read((char*)&(add->offset[i]), sizeof(uint32_t));
        if (add->key[i] == key) dest_index = i;
    }

    if (dest_index == -1) {

        in.close();

        delete [] add->key;
        delete [] add->offset;
        delete add;

        return "";
    }
    else {

        in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
        long long length = in.tellg(); // 获得文件总的字节数
        in.seekg(0, std::ifstream::beg); // 回到输入流的头部

        uint32_t destOffset = add->offset[dest_index];
        uint32_t destLength;
        if (dest_index == add->num - 1)
            destLength = length - destOffset;
        else
            destLength = add->offset[dest_index + 1] - destOffset;

        in.seekg(destOffset); // 对文件指针进行目标量的偏移
        char *str_val = new char[destLength + 1];
        in.read(str_val, destLength);
        str_val[destLength] = '\0';
        std::string val = str_val;
        delete [] str_val;

        in.close();

        delete [] add->key;
        delete [] add->offset;
        delete add;

        return val;
    }
}

// 默认是最开始 level0 调用的 compaction, 向 level1 进行下溢合并（不含参）, 该函数调用含参 compaction
void KVStore::compaction() {

    // 首先判断 level1 是否存在（若存在一定可以说明该层有若干文件）
    bool level1_exist = utils::dirExists(file + "/level1");

    std::vector<std::string> level0_files;
    utils::scanDir(file + "/level0", level0_files);
    std::vector<std::string>::iterator Iter;
    Iter = level0_files.begin();

    sst_buf* ptr1 = GetBuffer(file + "/level0/" + *Iter);
    Iter ++ ;
    sst_buf* ptr2 = GetBuffer(file + "/level0/" + *Iter);
    Iter ++ ;
    sst_buf* ptr3 = GetBuffer(file + "/level0/" + *Iter);

    uint64_t key_min = ptr1->min;
    uint64_t key_max = ptr1->max;
    uint64_t new_time = ptr1->time;
    if (ptr2->min < key_min) key_min = ptr2->min;
    if (ptr3->min < key_min) key_min = ptr3->min;
    if (ptr2->max > key_max) key_max = ptr2->max;
    if (ptr3->max > key_max) key_max = ptr3->max; // 获取 level0 的键值覆盖区间
    if (ptr2->time > new_time) new_time = ptr2->time;
    if (ptr3->time > new_time) new_time = ptr3->time; // 获取合并文件的最大时间戳作为新的时间戳

    // 将 level0 三份文件进行归并排序（处理重复键值），按 2MB 划分为新的 level1 文件（不可能继续下溢）
    std::list<std::pair<uint64_t, std::string>> list;

    for (uint64_t i=0; i<(key_max - key_min + 1); ++i) {
        std::pair<uint64_t, std::string> insert (key_min + i, "");
        list.push_back(insert);
    } // 初始化，将所有键的值设为空的 ""

    auto *time_record = new uint64_t [key_max - key_min + 1];
    for (uint64_t i=0; i<key_max - key_min + 1; ++i)
        time_record[i] = 0; //记录当前时间戳的数组，并且全部初始化为 0

    // 将三个文件中的键值对全部存入 list 中保存

    std::ifstream in;
    in.open(ptr1->path, std::ios::binary|std::ios::in);

    in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
    long long length = in.tellg(); // 获得文件总的字节数
    in.seekg(0, std::ifstream::beg); // 回到输入流的头部

    auto *add = new sst_buf;

    in.read((char*)&(add->time), sizeof(uint64_t));
    in.read((char*)&(add->num), sizeof(uint64_t));
    in.read((char*)&(add->min), sizeof(uint64_t));
    in.read((char*)&(add->max), sizeof(uint64_t));

    in.read((char*)&(add->arr), sizeof(add->arr));

    add->key = new uint64_t [add->num];
    add->offset = new uint32_t [add->num]; // 动态分配内存后记得释放堆空间

    for (int j=0; j < add->num; ++j) {
        in.read((char*)&(add->key[j]), sizeof(uint64_t));
        in.read((char*)&(add->offset[j]), sizeof(uint32_t));
    }

    for (uint64_t i=0; i<ptr1->num; ++i) {
        uint64_t key = ptr1->key[i];

        uint64_t dest_index = i;
        uint32_t destOffset = add->offset[dest_index];
        uint32_t destLength;
        if (dest_index == add->num - 1)
            destLength = length - destOffset;
        else
            destLength = add->offset[dest_index + 1] - destOffset;

        char *str_val = new char[destLength + 1];
        in.read(str_val, destLength);
        str_val[destLength] = '\0';
        std::string val = str_val;
        delete [] str_val;

        uint64_t time_index = key - key_min;
        if (ptr1->time > time_record[time_index]) {
            update_list(key, val, key_min, key_max, list);
            time_record[time_index] = ptr1->time;
        }
    }

    delete [] add->key;
    delete [] add->offset;
    delete add;

    in.close();

// ==========================================================================================

    in.open(ptr2->path, std::ios::binary|std::ios::in);

    in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
    length = in.tellg(); // 获得文件总的字节数
    in.seekg(0, std::ifstream::beg); // 回到输入流的头部

    add = new sst_buf;

    in.read((char*)&(add->time), sizeof(uint64_t));
    in.read((char*)&(add->num), sizeof(uint64_t));
    in.read((char*)&(add->min), sizeof(uint64_t));
    in.read((char*)&(add->max), sizeof(uint64_t));

    in.read((char*)&(add->arr), sizeof(add->arr));

    add->key = new uint64_t [add->num];
    add->offset = new uint32_t [add->num]; // 动态分配内存后记得释放堆空间

    for (int j=0; j < add->num; ++j) {
        in.read((char*)&(add->key[j]), sizeof(uint64_t));
        in.read((char*)&(add->offset[j]), sizeof(uint32_t));
    }

    for (uint64_t i=0; i<ptr2->num; ++i) {
        uint64_t key = ptr2->key[i];

        uint64_t dest_index = i;
        uint32_t destOffset = add->offset[dest_index];
        uint32_t destLength;
        if (dest_index == add->num - 1)
            destLength = length - destOffset;
        else
            destLength = add->offset[dest_index + 1] - destOffset;

        char *str_val = new char[destLength + 1];
        in.read(str_val, destLength);
        str_val[destLength] = '\0';
        std::string val = str_val;
        delete [] str_val;

        uint64_t time_index = key - key_min;
        if (ptr2->time > time_record[time_index]) {
            update_list(key, val, key_min, key_max, list);
            time_record[time_index] = ptr2->time;
        }
    }

    delete [] add->key;
    delete [] add->offset;
    delete add;

    in.close();

// =============================================================================================

    in.open(ptr3->path, std::ios::binary|std::ios::in);

    in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
    length = in.tellg(); // 获得文件总的字节数
    in.seekg(0, std::ifstream::beg); // 回到输入流的头部

    add = new sst_buf;

    in.read((char*)&(add->time), sizeof(uint64_t));
    in.read((char*)&(add->num), sizeof(uint64_t));
    in.read((char*)&(add->min), sizeof(uint64_t));
    in.read((char*)&(add->max), sizeof(uint64_t));

    in.read((char*)&(add->arr), sizeof(add->arr));

    add->key = new uint64_t [add->num];
    add->offset = new uint32_t [add->num]; // 动态分配内存后记得释放堆空间

    for (int j=0; j < add->num; ++j) {
        in.read((char*)&(add->key[j]), sizeof(uint64_t));
        in.read((char*)&(add->offset[j]), sizeof(uint32_t));
    }

    for (uint64_t i=0; i<ptr3->num; ++i) {
        uint64_t key = ptr3->key[i];

        uint64_t dest_index = i;
        uint32_t destOffset = add->offset[dest_index];
        uint32_t destLength;
        if (dest_index == add->num - 1)
            destLength = length - destOffset;
        else
            destLength = add->offset[dest_index + 1] - destOffset;

        char *str_val = new char[destLength + 1];
        in.read(str_val, destLength);
        str_val[destLength] = '\0';
        std::string val = str_val;
        delete [] str_val;

        uint64_t time_index = key - key_min;
        if (ptr3->time > time_record[time_index]) {
            update_list(key, val, key_min, key_max, list);
            time_record[time_index] = ptr3->time;
        }
    }

    delete [] add->key;
    delete [] add->offset;
    delete add;

    in.close();

// ===============================================================================================

    // 进行动态分配空间的释放以及原文件的删除
    utils::rmfile(ptr1->path.c_str());
    utils::rmfile(ptr2->path.c_str());
    utils::rmfile(ptr3->path.c_str());

    delete [] ptr1->key;
    delete [] ptr1->offset;
    delete ptr1;

    delete [] ptr2->key;
    delete [] ptr2->offset;
    delete ptr2;

    delete [] ptr3->key;
    delete [] ptr3->offset;
    delete ptr3;

    // case 1: 如果不存在 -> 获取 level0 的键值覆盖区间
    if (!level1_exist) {
        delete [] time_record;

        // 按 2MB 划分为新的 level1 文件（不可能继续下溢）, 更新缓存中的链表（新时间戳取 max -> 注意对于文件的重命名）
        utils::mkdir((file + "/level1").c_str());
        split(list, 1, new_time);
        return;
    }

    // case 2: 如果存在 -> level0 的文件全部有用信息已经存储至 list 中，level1 中的部分文件同样需要类似的 update 处理
    if (level1_exist) {
        // 在 level1 中寻找与该区间有交集的所有 sst 文件，合在一起进行归并排序
        expansion(list, new_time, 1, time_record);
        delete [] time_record;
        split(list, 1, new_time);

        // 判断 level1 是否会触发新的下溢
        bool flag = IsLevelFull("level1");

        if (!flag)
            return;
        else // 如果继续下溢，那么递归地调用 compaction 携参函数
            compaction(1);
    }
}

// 函数功能：根据已有的键值对 list, 在 cur_level 中找到有交集的 sst 文件并进行相关变量的更新或删除
void KVStore::expansion(std::list<std::pair<uint64_t, std::string>> &list, uint64_t &timeStamp, int cur_level,
                        uint64_t *time_record) {
    // 获得当前 list 中键值的取值范围(max/min)
    uint64_t key_max = list.back().first;
    uint64_t key_min = list.front().first;

    std::vector<std::string> all_files;
    utils::scanDir(file + "/level" + std::to_string(cur_level), all_files);
    std::vector<std::string>::iterator Iter;
    Iter = all_files.begin();

    while (Iter != all_files.end()) { // 每一层循环处理当前层的一个文件

        // 首先判断该文件是否与目标范围有交集，读取文件键值 min/max
        sst_buf* ptr = GetBuffer(file + "/level" + std::to_string(cur_level) + "/" + *Iter);
        uint64_t file_min = ptr->min;
        uint64_t file_max = ptr->max;

        int shadow_type = 0; // 1: left; 2: middle; 3: right; 4: cover.
        if ((file_min < key_min) && (file_max >= key_min) && (file_max <= key_max)) shadow_type = 1;
        if ((file_min >= key_min) && (file_max <= key_max)) shadow_type = 2;
        if ((file_min >= key_min) && (file_min <= key_max) && (file_max > key_max)) shadow_type = 3;
        if ((file_min < key_min) && (file_max > key_max)) shadow_type = 4;

        if (!shadow_type) { // 将堆中取出的结构体重新放回
            ptr->next = head->next;
            head->next = ptr;
            Iter ++; // 如果没有交集，可以直接跳过当前层的循环
            continue;
        }

        // 找到所有文件对应的最大时间戳作为最终合并后的时间戳
        if (ptr->time > timeStamp)
            timeStamp = ptr->time;

        if (shadow_type == 1) {
            // 首先将链表的缺失部分补全
            for (uint64_t i=key_min-1; i>=file_min; i--) {
                std::pair<uint64_t, std::string> insert (i, "");
                list.push_front(insert);
                if (i == 0) break;
            }
        }

        if (shadow_type == 3) {
            // 首先将链表的缺失部分补全
            for (uint64_t i=key_max+1; i<=file_max; i++) {
                std::pair<uint64_t, std::string> insert (i, "");
                list.push_back(insert);
            }
        }

        if (shadow_type == 4) {
            // 首先将链表的缺失部分补全
            for (uint64_t i=key_min-1; i>=file_min; i--) {
                std::pair<uint64_t, std::string> insert (i, "");
                list.push_front(insert);
                if (i == 0) break;
            }
            for (uint64_t i=key_max+1; i<=file_max; i++) {
                std::pair<uint64_t, std::string> insert (i, "");
                list.push_back(insert);
            }
        }

        std::ifstream in;
        in.open(ptr->path, std::ios::binary|std::ios::in);

        in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
        long long length = in.tellg(); // 获得文件总的字节数
        in.seekg(0, std::ifstream::beg); // 回到输入流的头部

        auto *add = new sst_buf;

        in.read((char*)&(add->time), sizeof(uint64_t));
        in.read((char*)&(add->num), sizeof(uint64_t));
        in.read((char*)&(add->min), sizeof(uint64_t));
        in.read((char*)&(add->max), sizeof(uint64_t));

        in.read((char*)&(add->arr), sizeof(add->arr));

        add->key = new uint64_t [add->num];
        add->offset = new uint32_t [add->num]; // 动态分配内存后记得释放堆空间

        for (int i=0; i<add->num; ++i) {
            in.read((char*)&(add->key[i]), sizeof(uint64_t));
            in.read((char*)&(add->offset[i]), sizeof(uint32_t));
        }

        // type 2: 原 list 范围内链表的更新需要与 time_record 时间戳比较
        if (shadow_type == 2) {
            for (uint64_t i=0; i<ptr->num; ++i) {
                uint64_t key = ptr->key[i];

                uint32_t destOffset = add->offset[i];
                uint32_t destLength;
                if (i == add->num - 1)
                    destLength = length - destOffset;
                else
                    destLength = add->offset[i + 1] - destOffset;

                char *str_val = new char[destLength + 1];
                in.read(str_val, destLength);
                str_val[destLength] = '\0';
                std::string val = str_val;
                delete [] str_val;

                uint64_t time_index = key - key_min;
                if (ptr->time > time_record[time_index]) {
                    time_record[time_index] = ptr->time;
                    update_list(key, val, key_min, key_max, list);
                }
            }
        }

        // type 1: list 扩充范围部分的键值对存储无需与 time_record 比较
        if (shadow_type == 1) {
            // 接下来遍历文件所有键值，进行键值对的更新
            for (uint64_t i=0; i<ptr->num; ++i) {
                uint64_t key = ptr->key[i];

                uint32_t destOffset = add->offset[i];
                uint32_t destLength;
                if (i == add->num - 1)
                    destLength = length - destOffset;
                else
                    destLength = add->offset[i + 1] - destOffset;

                char *str_val = new char[destLength + 1];
                in.read(str_val, destLength);
                str_val[destLength] = '\0';
                std::string val = str_val;
                delete [] str_val;

                uint64_t time_index = key - key_min;
                if (time_index < 0) {
                    update_list(key, val, key_min, key_max, list);
                    continue;
                }
                if (ptr->time > time_record[time_index]) {
                    time_record[time_index] = ptr->time;
                    update_list(key, val, key_min, key_max, list);
                }
            }
        }

        // type 3
        if (shadow_type == 3) {
            // 接下来遍历文件所有键值，进行键值对的更新
            for (uint64_t i=0; i<ptr->num; ++i) {
                uint64_t key = ptr->key[i];

                uint32_t destOffset = add->offset[i];
                uint32_t destLength;
                if (i == add->num - 1)
                    destLength = length - destOffset;
                else
                    destLength = add->offset[i + 1] - destOffset;

                char *str_val = new char[destLength + 1];
                in.read(str_val, destLength);
                str_val[destLength] = '\0';
                std::string val = str_val;
                delete [] str_val;

                if (key > key_max) {
                    update_list(key, val, key_min, key_max, list);
                    continue;
                }
                uint64_t time_index = key - key_min;
                if (ptr->time > time_record[time_index]) {
                    time_record[time_index] = ptr->time;
                    update_list(key, val, key_min, key_max, list);
                }
            }
        }

        // type 4
        if (shadow_type == 4) {
            // 接下来遍历文件所有键值，进行键值对的更新
            for (uint64_t i=0; i<ptr->num; ++i) {
                uint64_t key = ptr->key[i];

                uint32_t destOffset = add->offset[i];
                uint32_t destLength;
                if (i == add->num - 1)
                    destLength = length - destOffset;
                else
                    destLength = add->offset[i + 1] - destOffset;

                char *str_val = new char[destLength + 1];
                in.read(str_val, destLength);
                str_val[destLength] = '\0';
                std::string val = str_val;
                delete [] str_val;

                if ((key > key_max) || (key < key_min)) {
                    update_list(key, val, key_min, key_max, list);
                    continue;
                }
                uint64_t time_index = key - key_min;
                if (ptr->time > time_record[time_index]) {
                    time_record[time_index] = ptr->time;
                    update_list(key, val, key_min, key_max, list);
                }
            }
        }

        in.close();

        delete [] add->key;
        delete [] add->offset;
        delete add;

        // 如果有交集，该文件以及文件对应的缓存结构体都应该被删除
        utils::rmfile(ptr->path.c_str());
        delete [] ptr->key;
        delete [] ptr->offset;
        delete ptr;

        Iter++;
    }
}

// 函数参数 list: 储存所有的键值对，需要按照 2MB 分解为若干个 sst 文件
// 函数参数 level: 需要放置新产生文件的层数
// 函数参数 timeStamp: 新产生的文件具有相同的时间戳，需要再加后缀用以区分
// 函数功能：产生 sst 文件存储在磁盘上，对应的更新缓存中的链表
void KVStore::split(std::list<std::pair<uint64_t, std::string>> &list, int level, uint64_t timeStamp) {

    // 首先应该将 list 中所有的空字符串除去
    std::list<std::pair<uint64_t, std::string>>::iterator del_Iter;
    del_Iter = list.begin();
    for ( ; del_Iter != list.end(); ) {
        if ((*del_Iter).second.empty())
            del_Iter = list.erase(del_Iter);
        else
            del_Iter ++ ;
    }

    std::vector<std::string> tmp;
    int all_num = utils::scanDir(file + "/" + "level" + std::to_string(level), tmp);
    std::vector<std::string>::iterator Iter;
    Iter = tmp.begin();

    auto * find = new uint64_t [2*all_num];
    for (int i=0; i<all_num; ++i) { // 提取所有文件的时间戳以及同一时间戳的文件序号
        std::vector<std::string> res = split('_', *Iter);
        std::vector<std::string>::iterator Iter2;
        Iter2 = res.begin();
        Iter2++;
        find[2*i] = atoi((*Iter2).c_str());
        Iter2++;
        find[2*i+1] = atoi((Iter2)->c_str());
        Iter++;
    }

    uint64_t file_num = 1; // 记录同一时间戳对应的文件个数

    for (int i=0; i<all_num; ++i) {
        if (find[2*i]==timeStamp-1 && find[2*i+1]+1>file_num) {
            file_num = find[2*i+1] + 1;
        }
    }

    while (true) { // 每一层循环对应着一个 sst 文件的生成
        uint64_t pair_num = cal_split(list); // 键值对数目立即确定
        auto *add = new sst_buf;
        add->time = timeStamp;
        add->num = pair_num;
        add->key = new uint64_t [add->num];
        add->offset = new uint32_t [add->num];
        int *str_len = new int [add->num];

        std::list<std::pair<uint64_t, std::string>>::iterator Iter;
        Iter = list.begin();
        for (uint64_t i=0; i<add->num; ++i) {
            add->key[i] = (*Iter).first;
            str_len[i] = (int ) (*Iter).second.length();
            Iter ++;
        }

        uint32_t offset = 32 + 10240 + 12 * add->num; // 数据区的起始地址
        for (uint64_t i=0; i<add->num; ++i) {
            add->offset[i] = offset;
            offset += str_len[i];
        }
        delete [] str_len;

        for (uint64_t i=0; i<add->num; ++i) {
            unsigned int hash[4] = {0};
            MurmurHash3_x64_128(add->key + i, sizeof(uint64_t), 1, hash);
            add->arr[hash[0] % 81920] = true;
            add->arr[hash[1] % 81920] = true;
            add->arr[hash[2] % 81920] = true;
            add->arr[hash[3] % 81920] = true;
        }

        add->min = add->key[0];
        add->max = add->key[add->num-1];

        // 注意文件名与时间戳相差 1
        add->path = file + "/level" + std::to_string(level) + "/level" + std::to_string(level) + "_" + std::to_string(timeStamp-1) + "_" + std::to_string(file_num) + ".sst";

        std::ofstream out(add->path, std::ios::out|std::ios::app|std::ios::binary);
        out.write((char*)&add->time, sizeof(uint64_t));
        out.write((char*)&add->num, sizeof(uint64_t));
        out.write((char*)&add->min, sizeof(uint64_t));
        out.write((char*)&add->max, sizeof(uint64_t));

        out.write((char*)&add->arr, sizeof(add->arr));

        for (int j=0; j<add->num; ++j) {
            out.write((char*)&add->key[j], sizeof(uint64_t));
            out.write((char*)&add->offset[j], sizeof(uint32_t));
        }

        for (uint64_t i=0; i< add->num; ++i) { // 将 list 中对应 pair 弹出，将字符串写入文件
            std::string val = list.front().second;
            out.write(val.c_str(), (int)val.length());
            list.pop_front();
        }

        out.close();

        add->next = head->next;
        head->next = add;

        // 处理文件个数增量，循环终止条件判定
        file_num ++ ;

        if (list.empty())
            break;
    }
}

// 函数功能：计算链表从头开始至多多少个键值对可以组成一个 2MB 的 sst 文件
uint64_t KVStore::cal_split(std::list<std::pair<uint64_t, std::string>> &list) {
    uint64_t bytes_num = 10240 + 32;
    uint64_t pair_num = 0;

    std::list<std::pair<uint64_t, std::string>>::iterator Iter;
    Iter = list.begin();

    while (Iter != list.end()) {
        uint64_t bytes_add = 12; // 当前迭代器指向的键值对在插入后导致的字节数增量
        bytes_add += (*Iter).second.length();
        if (bytes_num + bytes_add <= 2 * 1024 * 1024) {
            pair_num ++ ;
            bytes_num += bytes_add;
            Iter ++ ;
        } else {
            break;
        }
    }

    return pair_num;
}

// 传递的参数 level 为触发下溢的层数，希望通过递归调用解决问题
void KVStore::compaction(int level) {

    // 在当前 level 层计算超额文件个数，并确定参与合并的文件（时间戳小、键值范围小）
    int merge_num = IsLevelFull("level" + std::to_string(level));
    std::vector<std::string> tmp;
    int all_num = utils::scanDir(file + "/" + "level" + std::to_string(level), tmp);
    std::vector<std::string>::iterator Iter;
    Iter = tmp.begin();

    // 当前层覆盖范围内所有的键值对链表，作为与下一层的接口
    std::list<std::pair<uint64_t, std::string>> list;
    // 将波及的文件以及对应的缓冲区结构体进行删除

    auto * find = new uint64_t [2*all_num];
    for (int i=0; i<all_num; ++i) { // 提取所有文件的时间戳以及同一时间戳的文件序号
        std::vector<std::string> res = split('_', *Iter);
        std::vector<std::string>::iterator Iter2;
        Iter2 = res.begin();
        Iter2++;
        find[2*i] = atoi((*Iter2).c_str());
        Iter2++;
        find[2*i+1] = atoi((Iter2)->c_str());
        Iter++;
    }

    auto * index = new int [merge_num];

    for (int i=0; i<merge_num; ++i) {
        int tmp_index = 0;
        uint64_t tmp_time = find[0];
        uint64_t tmp_file = find[1];
        for (int j=1; j<all_num; ++j) {
            if (find[2*j] < tmp_time) {
                tmp_index = j;
                tmp_time = find[2*j];
                tmp_file = find[2*j+1];
            }
            if (find[2*j]==tmp_time && find[2*j+1]<tmp_file) {
                tmp_index = j;
                tmp_file = find[2*j+1];
            }
        }
        index[i] = tmp_index;
        find[2*tmp_index] = ULONG_LONG_MAX;
        find[2*tmp_index + 1] = ULONG_LONG_MAX;
    }

    auto * merge_file = new sst_buf* [merge_num]; // 记得回收该指针空间

    for (int i=0; i<merge_num; ++i) {
        Iter = tmp.begin();
        for (int j=0; j<index[i]; ++j)
            Iter++;
        merge_file[i] = GetBuffer(file + "/level" + std::to_string(level) + "/" + *Iter);
    }

    uint64_t key_min = merge_file[0]->min;
    uint64_t key_max = merge_file[0]->max;
    // 当前层溢出文件的最大时间戳
    uint64_t new_time = merge_file[0]->time;

    for (int i=1; i<merge_num; ++i) {
        if (merge_file[i]->min < key_min) key_min = merge_file[i]->min;
        if (merge_file[i]->max > key_max) key_max = merge_file[i]->max;
        if (merge_file[i]->time > new_time) new_time = merge_file[i]->time;
    }

    for (uint64_t i=0; i<(key_max - key_min + 1); ++i) {
        std::pair<uint64_t, std::string> insert (key_min + i, "");
        list.push_back(insert);
    } // 初始化，将所有键的值设为空的 ""

    // 与链表 list 相对应的时间戳数组，为与下一层合并做准备
    auto *time_record = new uint64_t [key_max - key_min + 1];
    for (uint64_t i=0; i<key_max - key_min + 1; ++i)
        time_record[i] = 0; //记录当前时间戳的数组，并且全部初始化为 0

    for (int i=0; i<merge_num; ++i) {

        std::ifstream in;
        in.open(merge_file[i]->path, std::ios::binary|std::ios::in);

        in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
        long long length = in.tellg(); // 获得文件总的字节数
        in.seekg(0, std::ifstream::beg); // 回到输入流的头部

        auto *add = new sst_buf;

        in.read((char*)&(add->time), sizeof(uint64_t));
        in.read((char*)&(add->num), sizeof(uint64_t));
        in.read((char*)&(add->min), sizeof(uint64_t));
        in.read((char*)&(add->max), sizeof(uint64_t));

        in.read((char*)&(add->arr), sizeof(add->arr));

        add->key = new uint64_t [add->num];
        add->offset = new uint32_t [add->num]; // 动态分配内存后记得释放堆空间

        for (int j=0; j < add->num; ++j) {
            in.read((char*)&(add->key[j]), sizeof(uint64_t));
            in.read((char*)&(add->offset[j]), sizeof(uint32_t));
        }

        for (uint64_t j=0; j<merge_file[i]->num; ++j) {
            uint64_t key = merge_file[i]->key[j];
            uint64_t dest_index = j;
            uint32_t destOffset = add->offset[dest_index];
            uint32_t destLength;
            if (dest_index == add->num - 1)
                destLength = length - destOffset;
            else
                destLength = add->offset[dest_index + 1] - destOffset;

            char *str_val = new char[destLength + 1];
            in.read(str_val, destLength);
            str_val[destLength] = '\0';
            std::string val = str_val;
            delete [] str_val;

            uint64_t time_index = key - key_min;
            update_list(key, val, key_min, key_max, list);
            time_record[time_index] = merge_file[i]->time;
        }

        delete [] add->key;
        delete [] add->offset;
        delete add;

        in.close();
    }

    for (int i = 0; i < merge_num; ++i) {
        utils::rmfile((merge_file[i]->path).c_str());
    }

    for (int i=0; i<merge_num; ++i) {
        delete [] merge_file[i]->key;
        delete [] merge_file[i]->offset;
        delete merge_file[i];
    }

    delete [] merge_file;

    delete [] find;
    delete [] index;

    // 其次判断 level + 1 是否存在（若存在一定可以说明该层有若干文件）
    bool level_exist = utils::dirExists(file + "/level" + std::to_string(level+1));

    // case 1: 如果不存在 -> 获取 level 的键值覆盖区间
    if (!level_exist) {
        delete [] time_record;

        // 按 2MB 划分为新的 level1 文件（不可能继续下溢）, 更新缓存中的链表（新时间戳取 max -> 注意对于文件的重命名）
        utils::mkdir((file + "/level" + std::to_string(level+1)).c_str());
        split(list, level+1, new_time);
        return;
    }

    // case 2: 如果存在 -> 获取 level 的键值覆盖区间
    if (level_exist) {
        // 在 level+1 中寻找与该区间有交集的所有 sst 文件，合在一起进行归并排序
        expansion(list, new_time, level+1, time_record);
        delete [] time_record;
        split(list, level+1, new_time);

        // 判断 level+1 是否会触发新的下溢
        bool flag = IsLevelFull("level" + std::to_string(level+1));

        if (!flag)
            return;
        else // 如果继续下溢，那么递归地调用 compaction 携参函数
            compaction(level+1);
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    bool flag = false; // 插入后是否会超过限制
    int cur_bytes = MemTable.GetCurrentDataLength();
    cur_bytes = cur_bytes + 12 + (int) s.length();
    if (cur_bytes > 2 * 1024 * 1024) flag = true;

    if (!flag) {
        MemTable.Insert(key, s);
    } // 直接在跳表中插入
    else {
        WriteToDisk();
        MemTable.Insert(key, s);
    }
}

uint64_t KVStore::binarySearch(const uint64_t *a, uint64_t n, uint64_t target)
{
    uint64_t low = 0, high = n-1, middle;
    while(low <= high)
    {
        middle = low + (high - low) / 2;
        if(target == a[middle])
            return middle;
        else if(target > a[middle])
            low = middle + 1;
        else if(target < a[middle]) {
            if (middle == 0)
                return -1;
            else
                high = middle - 1;
        }
    }
    return -1;
} // 返回值为数组下标，-1表示没有找到

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    bool flag; // 在跳表中能否找到目标值
    std::string val;

    flag = MemTable.Search(key, val);
    if (flag) {
        if (val == "~DELETED~")
            return "";
        else return val;
    }
    else {
        // 遍历缓冲区的 sst_buf 查找
        sst_buf *find = head->next;
        sst_buf *dest = nullptr;
        uint64_t dest_index; // dest 为最后确定的 SSTable

        while (find != nullptr) {

            unsigned int hash[4] = {0};
            MurmurHash3_x64_128(&key, sizeof(uint64_t), 1, hash);

            bool exist = false; // 是否可能存在于当前 SSTable 中
            if (find->arr[hash[0] % 81920] && find->arr[hash[1] % 81920] && find->arr[hash[2] % 81920] && find->arr[hash[3] % 81920])
                exist = true;

            if (exist) {
                // 用二分查找法读取 offset 指向的内容
                uint64_t index = binarySearch(find->key, find->num, key);
                if (index == -1) {
                    find = find->next;
                    continue;
                }

                if (dest == nullptr) {
                    dest = find;
                    dest_index = index;
                }
                else { // 与最大时间戳进行比较并决定取舍
                    if (find->time > dest->time) {
                        dest = find;
                        dest_index = index;
                    }
                }
            }

            find = find->next;
        }

        if (dest == nullptr)
            return "";

        std::ifstream in;
        in.open(dest->path, std::ios::binary|std::ios::in);

        in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
        long long length = in.tellg(); // 获得文件总的字节数
        in.seekg(0, std::ifstream::beg); // 回到输入流的头部

        uint32_t destOffset = dest->offset[dest_index];
        uint32_t destLength;
        if (dest_index == dest->num - 1)
            destLength = length - destOffset;
        else
            destLength = dest->offset[dest_index + 1] - destOffset;

        in.seekg(destOffset); // 对文件指针进行目标量的偏移
        char *str_val = new char[destLength + 1];
        in.read(str_val, destLength);
        str_val[destLength] = '\0';
        val = str_val;
        delete [] str_val;

        in.close();

        // 找到最大时间戳对应的 value 值并返回
        if (val == "~DELETED~")
            return "";
        else return val;
    }
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    std::string target = get(key);
    if (target.empty())
        return false;
    else {
        put(key, "~DELETED~");
        return true;
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    // 清除跳表
    SKNode *n1 = MemTable.head->forwards[0];
    SKNode *n2;
    while (n1->type != SKNodeType::NIL) {
        n2 = n1->forwards[0];
        delete n1;
        n1 = n2;
    }
    for (int i = 0; i < MAX_LEVEL; ++i)
    {
        MemTable.head->forwards[i]->type = NIL;
    } // 重新将 head NIL 进行连接

    // 清除 SSTable 的缓存部分
    sst_buf *del = head;
    while (head != nullptr) {
        head = head->next;
        delete [] del->key;
        delete [] del->offset;
        delete del;
        del = head;
    }
    head = new sst_buf;
    head->next = nullptr; // 析构后需要配置头结点

    bool isEmpty;
    isEmpty = utils::dirExists(file + "/level0");
    int cur_level = -1; // 记录当前 level 的值

    while (true) { // 外层循环对应着文件夹的删除

        if (!isEmpty) {
            break;
        } else {
            cur_level ++ ;
        }

        std::vector<std::string> all_files; // 获取当前目录所有的文件名放入 vector 之中
        // 利用内置的 scanDir 函数获得当前目录下的所有文件 vector
        std::string level_no = std::to_string(cur_level);
        std::string dirname = file + "/level" + level_no;
        utils::scanDir(dirname, all_files);

        std::vector<std::string>::iterator Iter;
        Iter = all_files.begin();

        while (Iter != all_files.end()) { // 内层循环对应当前目录下所有 .sst 文件的删除
            std::string filename = file + "/level" + level_no + "/" + *Iter;
            utils::rmfile(filename.c_str());
            Iter ++ ;
        } // 循环结束可以获得一个空目录，接下来删除该空目录

        utils::rmdir(dirname.c_str());

        // 开始判断下一层文件夹是否存在
        std::string next_level = std::to_string(cur_level + 1);
        std::string next_dir = file + "/level" + next_level;
        isEmpty = utils::dirExists(next_dir);
    }
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
    for (uint64_t i=0; i<(key2-key1+1); ++i) {
        std::pair<uint64_t, std::string> insert (key1+i, "");
        list.push_back(insert);
    } // 初始化，将所有键的值设为空的 ""

    auto *time_record = new uint64_t [key2-key1+1];
    for (uint64_t i=0; i<key2-key1+1; ++i)
        time_record[i] = 0; //记录当前时间戳的数组，并且全部初始化为 0

    // 扫描 MemTable 将列表中的字符串更新
    SKNode *travel = MemTable.head->forwards[0];
    while (travel->type != SKNodeType::NIL) {
        if (travel->key>=key1 && travel->key<=key2) {
            update_list(travel->key, travel->val, key1, key2, list);
            time_record[travel->key - key1] = time_max + 1;
        }
        travel = travel->forwards[0];
    }

    sst_buf *ptr = head->next;
    while (ptr != nullptr) {
        std::ifstream in;
        in.open(ptr->path, std::ios::binary|std::ios::in);

        // 首先从文件数据区读到所有的字符串
        in.seekg(0, std::ifstream::end); // 将文件指针指向输入流的末尾
        long long length = in.tellg(); // 获得文件总的字节数
        in.seekg(0, std::ifstream::beg); // 回到输入流的头部

        auto *add = new sst_buf;

        in.read((char*)&(add->time), sizeof(uint64_t));
        in.read((char*)&(add->num), sizeof(uint64_t));
        in.read((char*)&(add->min), sizeof(uint64_t));
        in.read((char*)&(add->max), sizeof(uint64_t));

        in.read((char*)&(add->arr), sizeof(add->arr)); // 载入 Bloom Filter

        long long data_len = length - 10272 - 12 * (long long)add->num; // 此为数据区的总的字节个数

        add->key = new uint64_t [add->num];
        add->offset = new uint32_t [add->num];

        for (int i=0; i<add->num; ++i) {
            in.read((char*)&(add->key[i]), sizeof(uint64_t));
            in.read((char*)&(add->offset[i]), sizeof(uint32_t));
        }

        // 开始读入数据区字符串
        auto **str_ptr = new char* [add->num];
        for (int i = 0; i < add->num; ++i) {
            if (i != add->num-1) {
                char *str_val = new char[add->offset[i + 1] - add->offset[i] + 1];
                in.read(str_val, add->offset[i + 1] - add->offset[i]);
                str_val[add->offset[i + 1] - add->offset[i]] = '\0';
                data_len -= (add->offset[i + 1] - add->offset[i]);
                str_ptr[i] = str_val;
            } else {
                char *str_val = new char[data_len + 1];
                in.read(str_val, data_len);
                str_val[data_len] = '\0';
                str_ptr[i] = str_val;
            }
        }

        delete [] add->key;
        delete [] add->offset;
        delete add;

        // 需要遍历每个文件里的所有键，如果符合条件则会对于链表进行 update
        for (int i=0; i<ptr->num; ++i) {
            uint64_t target = ptr->key[i];
            if (target>=key1 && target<=key2 && ptr->time>time_record[target-key1]) {
                update_list(target, str_ptr[i], key1, key2, list);
                time_record[target-key1] = ptr->time;
            }
        }

        for (int i=0; i<ptr->num; ++i)
            delete [] str_ptr[i];
        delete [] str_ptr;

        in.close();

        ptr = ptr->next;
    }

    std::list<std::pair<uint64_t, std::string>>::iterator del_Iter;
    del_Iter = list.begin();
    for ( ; del_Iter != list.end(); ) {
        if ((*del_Iter).second.empty() || !std::strcmp((*del_Iter).second.c_str(), "~DELETED~"))
            del_Iter = list.erase(del_Iter);
        else
            del_Iter ++ ;
    }

    delete [] time_record;
}

void KVStore::update_list(uint64_t key, const std::string val, uint64_t key1, uint64_t key2,
                          std::list<std::pair<uint64_t, std::string>> &list) {
    key1 = list.front().first;
    // 在 list 中找到键值 key
    uint64_t count = key - key1; // 需要移动迭代器的次数
    std::list<std::pair<uint64_t, std::string>>::iterator Iter;
    Iter = list.begin();
    for (uint64_t i=0; i<count; ++i)
        Iter++;
    std::pair<uint64_t, std::string> insert (key, val);
    *Iter = insert;
}

// 判断当前目录是否已经装满 sst 文件，也就是会不会触发下溢
int KVStore::IsLevelFull(const std::string &dir) {

    std::vector<std::string> tmp;
    int cur_sst_num = utils::scanDir(file + "/" + dir, tmp); // 当前文件数目

    std::string cut = dir.substr(5);
    int cur_level_num = atoi(cut.c_str());
    int max_sst_num = 1 << (cur_level_num + 1); // 最大文件数目为 2 的幂次方

    if (cur_sst_num <= max_sst_num)
        return 0;
    else
        return (cur_sst_num - max_sst_num);
}

std::vector<std::string> KVStore::split(char c, std::string src) {
    std::vector<std::string> res;
    int sp = 0,fp=0;
    while(fp<src.length()){
        fp = src.find(c,sp);
        res.push_back(src.substr(sp,fp-sp));
        sp = fp + 1;
    }
    return res;
}
