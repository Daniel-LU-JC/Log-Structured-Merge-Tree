#include <iostream>
#include "kvstore.h"
#include "utils.h"

int main() {

    KVStore test("../data");



//    for (int i=10000; i<19999; ++i) {
//        test.put(i, std::string(i, 's'));
//        std::cout << i << std::endl;
//    }

//    for (int i=0; i<19999; i+=2) {
//        test.del(i);
//        std::cout << i << std::endl;
//    }

//    for (int i=0; i<100; ++i) {
//        std::cout << test.get(i) << std::endl;
//    }

//    std::list<std::pair<uint64_t, std::string> > list;
//
//    test.scan(0, 4095, list);

//    std::list<std::pair<uint64_t, std::string> >::iterator Iter;
//    Iter = list.begin();
//
//    for (; Iter!=list.end(); ++Iter) {
//        std::cout << (*Iter).first << std::endl;
//        std::cout << (*Iter).second << std::endl;
//        std::cout << "============================" << std::endl;
//    }
//
    test.reset();

    return 0;
}
