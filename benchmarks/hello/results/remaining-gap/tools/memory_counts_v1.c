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
__attribute__((constructor)) static void initialize(void) {
    const char *path = getenv("BASE_HTTP_MEMORY_COUNTS");
    if (!path) return;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0 || ftruncate(fd, 4 * sizeof(uint64_t))) _exit(90);
    void *memory = mmap(NULL, 4 * sizeof(uint64_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (memory == MAP_FAILED) _exit(91);
    counts = memory;
}
static void record(unsigned operation, size_t length) {
    if (!counts) return;
    atomic_fetch_add_explicit(&counts[operation * 2], 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&counts[operation * 2 + 1], length, memory_order_relaxed);
}
static void *counted_memcpy(void *target, const void *source, size_t length) {
    void *result = memcpy(target, source, length);
    record(0, length);
    return result;
}
static void *counted_memmove(void *target, const void *source, size_t length) {
    void *result = memmove(target, source, length);
    record(1, length);
    return result;
}
#define INTERPOSE(replacement, original) \
    __attribute__((used)) static const struct { const void *replace; const void *original; } \
    interpose_##original __attribute__((section("__DATA,__interpose"))) = \
    { (const void *)&replacement, (const void *)&original };
INTERPOSE(counted_memcpy, memcpy)
INTERPOSE(counted_memmove, memmove)
