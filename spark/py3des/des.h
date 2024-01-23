#ifndef DES_H
#define DES_H

#include <cstdint>
#include <string>

using namespace std;

class DES {
public:
    DES(uint64_t key);

    string padString(const string &str);

    string uint64ToHex(uint64_t value);

    uint64_t hexToUint64(const string &hex);

    string uint64ToString(uint64_t value);

    uint64_t stringToUint64(const string &str);

    uint64_t des(uint64_t block, bool decrypt);

    virtual uint64_t encrypt(uint64_t block);

    virtual uint64_t decrypt(uint64_t block);


protected:
    void keygen(uint64_t key);

    uint64_t ip(uint64_t block);

    uint64_t fp(uint64_t block);

    void feistel(uint32_t &L, uint32_t &R, uint32_t F);

    uint32_t f(uint32_t R, uint64_t k);

private:
    uint64_t sub_key[16]; // 48 bitow
};

#endif // DES_H