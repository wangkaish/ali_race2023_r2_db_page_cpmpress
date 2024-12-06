//
// Created by test on 10/23/23.
//

#ifndef PAGE_ENGINE_FLAT_MAP_H
#define PAGE_ENGINE_FLAT_MAP_H

#include <cstdint>
#include "def.h"
#include "util.h"

class FlatMap {

    static const int CAPACITY = FLAT_MAP_CAPACITY;
    static const int MASK = CAPACITY - 1;

    uint32_t array[CAPACITY * 2]{};

public:

    static int compute_slot(uint32_t key) {
        return (int) (mix32(key) & MASK);
    }

    static int next_slot(int slot) {
        return (slot + 1) & (MASK);
    }

    void put(uint32_t key, uint32_t value) {
        auto slot = compute_slot(key);
        for (;;) {
            int pos = slot << 1;
            if (array[pos] == 0) {
                array[pos] = key;
                array[pos + 1] = value;
                break;
            } else {
                slot = next_slot(slot);
            }
        }
    }

    int get(uint32_t key) {
        auto slot = compute_slot(key);
        for (;;) {
            int pos = slot << 1;
            if (array[pos] == 0) {
                return -1;
            } else if (array[pos] == key) {
                return (int) (array[pos + 1]);
            } else {
                slot = next_slot(slot);
            }
        }
    }

    void shiftConflictingKeys(int gapSlot) {
        // Perform shifts of conflicting keys to fill in the gap.
        int distance = 0;
        while (true) {
            int slot = (gapSlot + (++distance)) & MASK;
            auto existing = array[slot << 1];
            if (existing == 0) {
                break;
            }

            int idealSlot = compute_slot(existing);
            int shift = (slot - idealSlot) & MASK;
            if (shift >= distance) {
                // Entry at this position was originally at or before the gap slot.
                // Move the conflict-shifted entry to the gap's position and repeat the procedure
                // for any entries to the right of the current position, treating it
                // as the new gap.
                array[gapSlot << 1] = existing;
                array[(gapSlot << 1) + 1] = array[(slot << 1) + 1];
                array[slot << 1] = 0;
                gapSlot = slot;
                distance = 0;
            }
        }
    }

    void remove(uint32_t key) {
        auto base_slot = compute_slot(key);
        auto slot = base_slot;
        for (;;) {
            int pos = slot << 1;
            if (array[pos] == key) {
                array[pos] = 0;
                shiftConflictingKeys(slot);
                break;
            } else if (array[pos] == 0) {
                assert(false);
            } else {
                slot = next_slot(slot);
            }
        }
    }

    int size() {
        int size = 0;
        for (int i = 0; i < CAPACITY; ++i) {
            if (array[i << 1] != 0) {
                size++;
            }
        }
        return size;
    }

};


#endif //PAGE_ENGINE_FLAT_MAP_H
