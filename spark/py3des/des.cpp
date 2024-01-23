#include <iostream>
#include <sstream>
#include <iomanip>

#include "des.h"
#include "permutations.h"

DES::DES(uint64_t key) {
    keygen(key);
}

string DES::padString(const string &str) {
    string paddedStr = str;
    size_t paddingSize = 8 - (str.length() % 8);
    if (paddingSize != 8) {
        // uzupelnianie znakiem w hex oznaczajacym liczbe brakujacych znakow do wielokrotnosci 8
        paddedStr.append(paddingSize, (char) (paddingSize));
    }
    return paddedStr;
}

string DES::uint64ToHex(uint64_t value) {
    ostringstream oss;
    oss << hex << setw(16) << setfill('0') << value;
    return oss.str();
}

uint64_t DES::hexToUint64(const string &hex) {
    uint64_t result;
    istringstream iss(hex);
    iss >> std::hex >> result;
    return result;
}

string DES::uint64ToString(uint64_t value) {
    string result;
    for (int i = 0; i < 8; ++i) {
        char byte = static_cast<char>((value >> ((7 - i) * 8)) & 0xFF);// zamiana koncowkowosci
        result.push_back(byte);
    }
    return result;
}

uint64_t DES::stringToUint64(const string &str) {
    if (str.length() != 8) {
        return 0;
    }

    uint64_t result = 0;
    for (int i = 0; i < 8; ++i) {
        result |= static_cast<uint64_t>(static_cast<unsigned char>(str[i])) << ((7 - i) * 8); // zamiana koncowkowosci
    }

    return result;
}

uint64_t DES::encrypt(uint64_t block) {
    return des(block, false);
}

uint64_t DES::decrypt(uint64_t block) {
    return des(block, true);
}

void DES::keygen(uint64_t key) {
    // przygotowanie klucza - usuniecie bitow parzystosci
    uint64_t permuted_choice_1 = 0; // 56 bity
    for (uint8_t i = 0; i < 56; i++) {
        permuted_choice_1 <<= 1;
        permuted_choice_1 |= (key >> (64 - PC1[i])) & LB64_MASK;
    }

    // przytgotowanie 28 bitowych polowek klucza
    uint32_t C = (uint32_t) ((permuted_choice_1 >> 28) & 0x000000000fffffff);
    uint32_t D = (uint32_t) (permuted_choice_1 & 0x000000000fffffff);

    // obliczanie 16 kluczy dla kazdej rundy
    for (uint8_t i = 0; i < 16; i++) {
        // przesuniecia bitowe polowek w kazdej rundzie
        for (uint8_t j = 0; j < ITERATION_SHIFT[i]; j++) {
            C = (0x0fffffff & (C << 1)) | (0x00000001 & (C >> 27));
            D = (0x0fffffff & (D << 1)) | (0x00000001 & (D >> 27));
        }

        uint64_t permuted_choice_2 = (((uint64_t) C) << 28) | (uint64_t) D;

        // przygotowanie 48 bitowego klucza poprzez permutacje kompresujaca
        sub_key[i] = 0; // 48 bits (2*24)
        for (uint8_t j = 0; j < 48; j++) {
            sub_key[i] <<= 1;
            sub_key[i] |= (permuted_choice_2 >> (56 - PC2[j])) & LB64_MASK;
        }
    }
}

uint64_t DES::des(uint64_t block, bool decrypt) {
    // permutacja poczatkowa
    block = ip(block);

    // podzial bloku wejsciowego na lewa i prawa czesc po 32 bity
    uint32_t left = block >> 32;
    uint32_t right = block & 0xFFFFFFFF;

    // 16 cykli feistela
    for (uint8_t i = 0; i < 16; i++) {
        uint32_t F = decrypt ? f(right, sub_key[15 - i]) : f(right, sub_key[i]);
        feistel(left, right, F);
    }

    // zamiana prawej i lewej strony miejscami
    block = ((uint64_t) right << 32) | left;

    // permutacja koncowa
    return fp(block);
}

uint64_t DES::ip(uint64_t block) {
    uint64_t result = 0;
    for (uint8_t i = 0; i < 64; i++) {
        result <<= 1;
        result |= (block >> (64 - IP[i])) & LB64_MASK;
    }
    return result;
}

uint64_t DES::fp(uint64_t block) {
    uint64_t result = 0;
    for (uint8_t i = 0; i < 64; i++) {
        result <<= 1;
        result |= (block >> (64 - FP[i])) & LB64_MASK;
    }
    return result;
}

void DES::feistel(uint32_t &L, uint32_t &R, uint32_t F) {
    uint32_t temp = R;
    R = L ^ F;
    L = temp;
}

// funkcja f w sieci feistela
uint32_t DES::f(uint32_t R, uint64_t k) {
    // permutacja rozszerzajca 32 bity na 48 bitow
    uint64_t s_input = 0;
    for (uint8_t i = 0; i < 48; i++) {
        s_input <<= 1;
        s_input |= (uint64_t) ((R >> (32 - EXPANSION[i])) & LB32_MASK);
    }

    // operacja XOR prawej czesci z kluczem
    s_input = s_input ^ k;

    // operacje S-blokow i wyscie 32 bitowe
    uint32_t s_output = 0;
    for (uint8_t i = 0; i < 8; i++) {
        // Outer bits
        char row = (char) ((s_input & (0x0000840000000000 >> 6 * i)) >> (42 - 6 * i));
        row = (row >> 4) | (row & 0x01);

        // Middle 4 bits of input
        char column = (char) ((s_input & (0x0000780000000000 >> 6 * i)) >> (43 - 6 * i));

        s_output <<= 4;
        s_output |= (uint32_t) (SBOX[i][16 * row + column] & 0x0f);
    }

    // permutacje P-blokow 32 bitowe
    uint32_t f_result = 0;
    for (uint8_t i = 0; i < 32; i++) {
        f_result <<= 1;
        f_result |= (s_output >> (32 - PBOX[i])) & LB32_MASK;
    }

    return f_result;
}
