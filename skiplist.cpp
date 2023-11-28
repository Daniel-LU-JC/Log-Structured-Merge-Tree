#include <iostream>
#include <stdlib.h>

#include "skiplist.h"

double SkipList::my_rand()
{
    s = (16807 * s) % 2147483647ULL;
    return (s + 0.0) / 2147483647ULL;
}

int SkipList::randomLevel()
{
    int result = 1;
    while (result < MAX_LEVEL && my_rand() < 0.5)
    {
        ++result;
    }
    return result;
}

void SkipList::Insert(uint64_t key, std::string value)
{
    SKNode* travel = head;
    int level = MAX_LEVEL - 1;

    SKNode* update[MAX_LEVEL];
    for (int i=0; i<MAX_LEVEL; ++i)
        update[i] = head;

    for ( ; level>=0; level--) {
        while ((travel->forwards[level] != NIL) && (travel->forwards[level]->key < key)) {
            travel = travel->forwards[level];
            for (int i = level; i >= 0 ; --i) {
                update[i] = travel;
            }
        }
    }

    if (travel->forwards[0]->key == key) {
        dataLength -= (int) (travel->forwards[0]->val).length();
        travel->forwards[0]->val = value;
        dataLength += (int) value.length();
        return;
    } // deal with the case of updating

    int new_level = randomLevel();
    auto* new_node = new SKNode(key, value, NORMAL); // really needs to insert
    for (int i = 0; i < new_level; ++i) {
        new_node->forwards[i] = update[i]->forwards[i];
        update[i]->forwards[i] = new_node;
    }

    dataLength += 12 + (int) value.length();
}

bool SkipList::Search(uint64_t key, std::string &str_ptr) const
{
    SKNode* travel = head;
    int level = MAX_LEVEL - 1;

    for ( ; level>=0; level--)
        while ((travel->forwards[level] != NIL) && (travel->forwards[level]->key < key)) {
            travel = travel->forwards[level];
        }

    travel = travel->forwards[0];

    if (travel->key == key) {
        str_ptr = travel->val;
        return true;
    }
    else {
        return false;
    }
}

void SkipList::Display()
{
    for (int i = MAX_LEVEL - 1; i >= 0; --i)
    {
        std::cout << "Level " << i + 1 << ":h";
        SKNode *node = head->forwards[i];
        while (node->type != SKNodeType::NIL)
        {
            std::cout << "-->(" << node->key << "," << node->val << ")";
            node = node->forwards[i];
        }

        std::cout << "-->N" << std::endl;
    }
}
