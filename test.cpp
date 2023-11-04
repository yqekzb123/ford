#include <iostream>
#include <cstdint>

int main(){
    uint64_t* a = new(uint64_t);

    *a = 1;
    *a = *a | 0xffff000000000000;
    std::cout << *a;    
    return 0;
}