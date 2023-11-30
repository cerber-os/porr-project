#include "des3.h"

DES3::DES3(uint64_t key, uint64_t k1, uint64_t k2, uint64_t k3) :
        DES(key), des1(k1), des2(k2), des3(k3) {
}

uint64_t DES3::encrypt(uint64_t block) {
    return des3.encrypt(des2.decrypt(des1.encrypt(block)));
}

uint64_t DES3::decrypt(uint64_t block) {
    return des1.decrypt(des2.encrypt(des3.decrypt(block)));
}