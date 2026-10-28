// macOS diagnostic only: count libc I/O on accepted sockets using dyld
// interposition. Never use an instrumented process for throughput comparisons.
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

enum { READ, READV, RECV, RECVFROM, RECVMSG, WRITE, WRITEV, SEND, SENDTO, SENDMSG, OPERATIONS };
enum { CALLS, POSITIVE, WOULD_BLOCK, OTHER_ERROR, BYTES, METRICS };
static _Atomic uint64_t (*counts)[METRICS];
static _Atomic unsigned char accepted[65536];
static _Thread_local unsigned depth;

#define INTERPOSE(replacement, original) \
    __attribute__((used)) static const struct { const void *replace; const void *original; } \
    interpose_##original __attribute__((section("__DATA,__interpose"))) = \
    { (const void *)&replacement, (const void *)&original };

__attribute__((constructor)) static void initialize(void) {
    const char *path = getenv("BASE_HTTP_SOCKET_COUNTS");
    if (!path) return;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) _exit(90);
    size_t size = OPERATIONS * METRICS * sizeof(uint64_t);
    if (ftruncate(fd, size)) _exit(91);
    void *memory = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    close(fd);
    if (memory == MAP_FAILED) _exit(92);
    counts = memory;
}

static void record(int operation, int fd, ssize_t result, int error) {
    if (!counts || fd < 0 || fd >= 65536 || !atomic_load_explicit(&accepted[fd], memory_order_relaxed)) return;
    atomic_fetch_add_explicit(&counts[operation][CALLS], 1, memory_order_relaxed);
    if (result > 0) {
        atomic_fetch_add_explicit(&counts[operation][POSITIVE], 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&counts[operation][BYTES], result, memory_order_relaxed);
    } else if (result < 0) {
        int metric = (error == EAGAIN || error == EWOULDBLOCK) ? WOULD_BLOCK : OTHER_ERROR;
        atomic_fetch_add_explicit(&counts[operation][metric], 1, memory_order_relaxed);
    }
}

static int counted_accept(int fd, struct sockaddr *address, socklen_t *length) {
    int result = accept(fd, address, length);
    int error = errno;
    if (result >= 0 && result < 65536) atomic_store_explicit(&accepted[result], 1, memory_order_relaxed);
    errno = error;
    return result;
}
INTERPOSE(counted_accept, accept)

static int counted_close(int fd) {
    if (fd >= 0 && fd < 65536) atomic_store_explicit(&accepted[fd], 0, memory_order_relaxed);
    return close(fd);
}
INTERPOSE(counted_close, close)

#define WRAP(name, operation, parameters, arguments) \
    static ssize_t counted_##name parameters { \
        unsigned nested = depth++; \
        ssize_t result = name arguments; \
        int error = errno; \
        if (!nested) record(operation, fd, result, error); \
        --depth; \
        errno = error; \
        return result; \
    } \
    INTERPOSE(counted_##name, name)

WRAP(read, READ, (int fd, void *bytes, size_t length), (fd, bytes, length))
WRAP(readv, READV, (int fd, const struct iovec *vectors, int count), (fd, vectors, count))
WRAP(recv, RECV, (int fd, void *bytes, size_t length, int flags), (fd, bytes, length, flags))
WRAP(recvfrom, RECVFROM,
     (int fd, void *bytes, size_t length, int flags, struct sockaddr *address, socklen_t *address_length),
     (fd, bytes, length, flags, address, address_length))
WRAP(recvmsg, RECVMSG, (int fd, struct msghdr *message, int flags), (fd, message, flags))
WRAP(write, WRITE, (int fd, const void *bytes, size_t length), (fd, bytes, length))
WRAP(writev, WRITEV, (int fd, const struct iovec *vectors, int count), (fd, vectors, count))
WRAP(send, SEND, (int fd, const void *bytes, size_t length, int flags), (fd, bytes, length, flags))
WRAP(sendto, SENDTO,
     (int fd, const void *bytes, size_t length, int flags, const struct sockaddr *address, socklen_t address_length),
     (fd, bytes, length, flags, address, address_length))
WRAP(sendmsg, SENDMSG, (int fd, const struct msghdr *message, int flags), (fd, message, flags))
