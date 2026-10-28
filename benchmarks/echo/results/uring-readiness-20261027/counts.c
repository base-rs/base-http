// Linux diagnostic only. Instrumented rates are not performance evidence.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <signal.h>
#include <unistd.h>

enum { READ, READV, RECV, RECVFROM, RECVMSG, WRITE, WRITEV, SEND, SENDTO, SENDMSG,
       EPOLL_WAIT, EPOLL_PWAIT, EPOLL_PWAIT2, EPOLL_CTL, ACCEPT, ACCEPT4, OPS };
enum { CALLS, POSITIVE, WOULD_BLOCK, ERROR, BYTES, ZERO, METRICS };
static _Atomic uint64_t (*counts)[METRICS];
static _Atomic unsigned char accepted[65536];
static _Thread_local unsigned depth;

#define DECLARE(name) static __typeof__(name) *next_##name;
DECLARE(read) DECLARE(readv) DECLARE(recv) DECLARE(recvfrom) DECLARE(recvmsg)
DECLARE(write) DECLARE(writev) DECLARE(send) DECLARE(sendto) DECLARE(sendmsg)
DECLARE(epoll_wait) DECLARE(epoll_pwait) DECLARE(epoll_pwait2) DECLARE(epoll_ctl)
DECLARE(accept) DECLARE(accept4) DECLARE(close)
#define RESOLVE(name) *(void **)(&next_##name) = dlsym(RTLD_NEXT, #name);

__attribute__((constructor)) static void initialize(void) {
    RESOLVE(read) RESOLVE(readv) RESOLVE(recv) RESOLVE(recvfrom) RESOLVE(recvmsg)
    RESOLVE(write) RESOLVE(writev) RESOLVE(send) RESOLVE(sendto) RESOLVE(sendmsg)
    RESOLVE(epoll_wait) RESOLVE(epoll_pwait) RESOLVE(epoll_pwait2) RESOLVE(epoll_ctl)
    RESOLVE(accept) RESOLVE(accept4) RESOLVE(close)
    const char *path = getenv("BASE_HTTP_SYSCALL_COUNTS");
    if (!path) return;
    int fd = open(path, O_RDWR | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) _exit(90);
    size_t size = OPS * METRICS * sizeof(uint64_t);
    if (ftruncate(fd, size)) _exit(91);
    void *memory = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    next_close(fd);
    if (memory == MAP_FAILED) _exit(92);
    counts = memory;
}

static void record(int operation, ssize_t result, int error) {
    if (!counts) return;
    atomic_fetch_add_explicit(&counts[operation][CALLS], 1, memory_order_relaxed);
    if (result > 0) {
        atomic_fetch_add_explicit(&counts[operation][POSITIVE], 1, memory_order_relaxed);
        atomic_fetch_add_explicit(&counts[operation][BYTES], result, memory_order_relaxed);
    } else if (result == 0) {
        atomic_fetch_add_explicit(&counts[operation][ZERO], 1, memory_order_relaxed);
    } else {
        int metric = (error == EAGAIN || error == EWOULDBLOCK) ? WOULD_BLOCK : ERROR;
        atomic_fetch_add_explicit(&counts[operation][metric], 1, memory_order_relaxed);
    }
}

int accept(int fd, struct sockaddr *address, socklen_t *length) {
    int result = next_accept(fd, address, length), saved = errno;
    if (result >= 0 && result < 65536) atomic_store_explicit(&accepted[result], 1, memory_order_relaxed);
    record(ACCEPT, result, saved); errno = saved; return result;
}
int accept4(int fd, struct sockaddr *address, socklen_t *length, int flags) {
    int result = next_accept4(fd, address, length, flags), saved = errno;
    if (result >= 0 && result < 65536) atomic_store_explicit(&accepted[result], 1, memory_order_relaxed);
    record(ACCEPT4, result, saved); errno = saved; return result;
}
int close(int fd) {
    if (!next_close) RESOLVE(close)
    if (fd >= 0 && fd < 65536) atomic_store_explicit(&accepted[fd], 0, memory_order_relaxed);
    return next_close(fd);
}

#define WRAP(name, operation, parameters, arguments) \
    ssize_t name parameters { \
        unsigned nested = depth++; \
        ssize_t result = next_##name arguments; \
        int saved = errno; \
        if (!nested && fd >= 0 && fd < 65536 && atomic_load_explicit(&accepted[fd], memory_order_relaxed)) \
            record(operation, result, saved); \
        --depth; errno = saved; return result; \
    }
WRAP(read, READ, (int fd, void *bytes, size_t length), (fd, bytes, length))
WRAP(readv, READV, (int fd, const struct iovec *vectors, int count), (fd, vectors, count))
WRAP(recv, RECV, (int fd, void *bytes, size_t length, int flags), (fd, bytes, length, flags))
WRAP(recvfrom, RECVFROM, (int fd, void *bytes, size_t length, int flags, struct sockaddr *address, socklen_t *address_length), (fd, bytes, length, flags, address, address_length))
WRAP(recvmsg, RECVMSG, (int fd, struct msghdr *message, int flags), (fd, message, flags))
WRAP(write, WRITE, (int fd, const void *bytes, size_t length), (fd, bytes, length))
WRAP(writev, WRITEV, (int fd, const struct iovec *vectors, int count), (fd, vectors, count))
WRAP(send, SEND, (int fd, const void *bytes, size_t length, int flags), (fd, bytes, length, flags))
WRAP(sendto, SENDTO, (int fd, const void *bytes, size_t length, int flags, const struct sockaddr *address, socklen_t address_length), (fd, bytes, length, flags, address, address_length))
WRAP(sendmsg, SENDMSG, (int fd, const struct msghdr *message, int flags), (fd, message, flags))

int epoll_wait(int fd, struct epoll_event *events, int count, int timeout) {
    int result = next_epoll_wait(fd, events, count, timeout), saved = errno;
    record(EPOLL_WAIT, result, saved); errno = saved; return result;
}
int epoll_pwait(int fd, struct epoll_event *events, int count, int timeout, const sigset_t *mask) {
    int result = next_epoll_pwait(fd, events, count, timeout, mask), saved = errno;
    record(EPOLL_PWAIT, result, saved); errno = saved; return result;
}
int epoll_pwait2(int fd, struct epoll_event *events, int count, const struct timespec *timeout, const sigset_t *mask) {
    int result = next_epoll_pwait2(fd, events, count, timeout, mask), saved = errno;
    record(EPOLL_PWAIT2, result, saved); errno = saved; return result;
}
int epoll_ctl(int fd, int operation, int socket, struct epoll_event *event) {
    int result = next_epoll_ctl(fd, operation, socket, event), saved = errno;
    record(EPOLL_CTL, result, saved); errno = saved; return result;
}
