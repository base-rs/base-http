// Fixed-rate Linux raw-control client. Validates every complete response.
#define _GNU_SOURCE
#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <time.h>
#include <unistd.h>

static const char response[] = "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 13\r\n\r\nHello, world!";
static char request[256];
static size_t request_size;
static int port, connections, rate, duration;
static uint64_t origin;
static pthread_barrier_t barrier;
static void fail(const char *message) { perror(message); exit(1); }
static uint64_t now(void) {
    struct timespec time; if (clock_gettime(CLOCK_MONOTONIC, &time)) fail("clock");
    return (uint64_t)time.tv_sec * 1000000000 + time.tv_nsec;
}
struct connection { int fd, busy; size_t used; char bytes[sizeof(response)]; };
struct worker { int index; uint64_t sent, completed, missed; };

static void *work(void *argument) {
    struct worker *worker = argument;
    int cpus[] = {2, 3, 6, 7}; cpu_set_t cpuset; CPU_ZERO(&cpuset); CPU_SET(cpus[worker->index], &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset)) fail("affinity");
    int queue = epoll_create1(EPOLL_CLOEXEC); if (queue < 0) fail("epoll_create");
    int count = connections / 4;
    struct connection *sockets = calloc((size_t)count, sizeof(*sockets));
    if (!sockets) fail("calloc");
    struct sockaddr_in address = {.sin_family=AF_INET, .sin_port=htons((uint16_t)port)};
    if (inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) != 1) fail("address");
    for (int i = 0; i < count; i++) {
        int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0), one = 1;
        if (fd < 0 || setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one)) || connect(fd, (struct sockaddr *)&address, sizeof(address))) fail("connect");
        sockets[i].fd = fd;
        struct epoll_event event = {.events=EPOLLIN|EPOLLRDHUP, .data.u32=(uint32_t)i};
        if (epoll_ctl(queue, EPOLL_CTL_ADD, fd, &event)) fail("epoll_ctl");
    }
    pthread_barrier_wait(&barrier); pthread_barrier_wait(&barrier);
    uint64_t step = 1000000000ULL / (uint64_t)(rate / 4), next = origin + (uint64_t)worker->index * (1000000000ULL / (uint64_t)rate);
    uint64_t end = origin + (uint64_t)duration * 1000000000;
    int cursor = 0;
    for (;;) {
        uint64_t current = now();
        while (next < end && current >= next) {
            struct connection *socket = &sockets[cursor]; cursor = (cursor + 1) % count;
            if (socket->busy) worker->missed++;
            else {
                ssize_t n = send(socket->fd, request, request_size, MSG_DONTWAIT|MSG_NOSIGNAL);
                if (n != (ssize_t)request_size) fail("complete send");
                socket->busy = 1; socket->used = 0; worker->sent++;
            }
            next += step;
        }
        if (current >= end && worker->sent == worker->completed) break;
        if (current >= end + 2000000000ULL) { errno=ETIMEDOUT; fail("drain"); }
        uint64_t deadline = next < end ? next : end + 2000000000ULL;
        uint64_t wait = deadline > current ? deadline - current : 0;
        struct timespec timeout = {.tv_sec=(time_t)(wait/1000000000), .tv_nsec=(long)(wait%1000000000)};
        struct epoll_event events[64];
        int n = epoll_pwait2(queue, events, 64, &timeout, NULL);
        if (n < 0) { if (errno == EINTR) continue; fail("epoll_wait"); }
        for (int i = 0; i < n; i++) {
            struct connection *socket = &sockets[events[i].data.u32];
            if (!socket->busy) { errno=EPROTO; fail("unsolicited response"); }
            ssize_t got = recv(socket->fd, socket->bytes + socket->used, sizeof(response)-1-socket->used, MSG_DONTWAIT);
            if (got < 0 && errno == EAGAIN) continue;
            if (got <= 0) fail("response read");
            socket->used += (size_t)got;
            if (socket->used == sizeof(response)-1) {
                if (memcmp(socket->bytes, response, sizeof(response)-1)) { errno=EPROTO; fail("response content"); }
                socket->busy=0; worker->completed++;
            }
        }
    }
    for (int i=0;i<count;i++) close(sockets[i].fd);
    close(queue); free(sockets); return NULL;
}
int main(int argc, char **argv) {
    if (argc != 5) { fprintf(stderr,"usage: fixed_load PORT CONNECTIONS RATE DURATION\n"); return 2; }
    port=atoi(argv[1]);connections=atoi(argv[2]);rate=atoi(argv[3]);duration=atoi(argv[4]);
    if (port<=0||port>65535||connections<4||connections%4||rate<=0||rate%4||duration<=0) return 2;
    request_size=(size_t)snprintf(request,sizeof(request),"GET / HTTP/1.1\r\nHost: 127.0.0.1:%d\r\n\r\n",port);
    pthread_barrier_init(&barrier,NULL,5);
    pthread_t threads[4]; struct worker workers[4]={0};
    for (int i=0;i<4;i++) { workers[i].index=i; if (pthread_create(&threads[i],NULL,work,&workers[i])) fail("thread"); }
    pthread_barrier_wait(&barrier);origin=now()+50000000;pthread_barrier_wait(&barrier);
    uint64_t sent=0,completed=0,missed=0;
    for (int i=0;i<4;i++) { pthread_join(threads[i],NULL);sent+=workers[i].sent;completed+=workers[i].completed;missed+=workers[i].missed; }
    printf("{\"requests\":%llu,\"sent\":%llu,\"missed\":%llu,\"target_rate\":%d,\"duration\":%d}\n",(unsigned long long)completed,(unsigned long long)sent,(unsigned long long)missed,rate,duration);
    return sent==completed && completed+missed==(uint64_t)rate*(uint64_t)duration ? 0 : 1;
}
