// macOS diagnostic: exported libc copy calls/bytes, excluding inlined copies.
// Never use an interposed process for throughput measurements.
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

static _Atomic uint64_t *counts;
enum { HISTOGRAM_END = 4 + 4097, SITES = 64, WORDS = HISTOGRAM_END + SITES * 3 };
__attribute__((constructor)) static void initialize(void) {
    const char *path = getenv("BASE_HTTP_MEMORY_COUNTS");
    if (!path) return;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0 || ftruncate(fd, WORDS * sizeof(uint64_t))) _exit(90);
    void *memory = mmap(NULL, WORDS * sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (memory == MAP_FAILED) _exit(91);
    counts = memory;
}
static void record(unsigned operation, size_t length, uintptr_t caller) {
    if (!counts) return;
    atomic_fetch_add_explicit(&counts[operation * 2], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&counts[operation * 2 + 1], length, memory_order_relaxed);
    atomic_fetch_add_explicit(&counts[4 + (length < 4096 ? length : 4096)], 1, memory_order_relaxed);
    if (length >= 256) {
        uint64_t key = caller;
        unsigned first = (unsigned)((key >> 2) ^ (key >> 10)) % SITES;
        for (unsigned probe = 0; probe < SITES; ++probe) {
            unsigned slot = HISTOGRAM_END + ((first + probe) % SITES) * 3;
            uint64_t observed = atomic_load_explicit(&counts[slot], memory_order_relaxed);
            if (observed == 0) {
                atomic_compare_exchange_strong_explicit(&counts[slot], &observed, key, memory_order_relaxed, memory_order_relaxed);
                observed = atomic_load_explicit(&counts[slot], memory_order_relaxed);
            }
            if (observed == key) {
                atomic_fetch_add_explicit(&counts[slot + 1], 1, memory_order_relaxed);
                atomic_fetch_add_explicit(&counts[slot + 2], length, memory_order_relaxed);
                break;
            }
        }
    }
}
static void *counted_memcpy(void *target, const void *source, size_t length) {
    void *result = memcpy(target, source, length);
    record(0, length, (uintptr_t)__builtin_extract_return_addr(__builtin_return_address(0)));
    return result;
}
static void *counted_memmove(void *target, const void *source, size_t length) {
    void *result = memmove(target, source, length);
    record(1, length, (uintptr_t)__builtin_extract_return_addr(__builtin_return_address(0)));
    return result;
}
#define INTERPOSE(replacement, original) \
    __attribute__((used)) static const struct { const void *replace; const void *original; } \
    interpose_##original __attribute__((section("__DATA,__interpose"))) = \
    { (const void *)&replacement, (const void *)&original };
INTERPOSE(counted_memcpy, memcpy)
INTERPOSE(counted_memmove, memmove)
