/*
 * Copyright (c) 2019 Mellanox Technologies, Inc.  All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the
 * OpenIB.org BSD license below:
 *
 *     Redistribution and use in source and binary forms, with or
 *     without modification, are permitted provided that the following
 *     conditions are met:
 *
 *      - Redistributions of source code must retain the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

#define _GNU_SOURCE
#include <arpa/inet.h>
#include <getopt.h>
#include <malloc.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "assert.h"
#include "gpu_direct_rdma_access.h"
#include "gpu_mem_util.h"
#include "utils.h"

#define MAX_SGES 512
#define ACK_MSG "rdma_task completed"
#define PACKAGE_TYPES 2

extern int debug;
extern int debug_fast_path;

#define DEBUG_LOG \
  if (debug) printf
#define DEBUG_LOG_FAST_PATH \
  if (debug_fast_path) printf
#define FDEBUG_LOG \
  if (debug) fprintf
#define FDEBUG_LOG_FAST_PATH \
  if (debug_fast_path) sprintf
#define SDEBUG_LOG \
  if (debug) fprintf
#define SDEBUG_LOG_FAST_PATH \
  if (debug_fast_path) sprintf

struct user_params {
  uint32_t task;
  int persistent;
  int port;
  unsigned long size;
  int iters;
  int num_sges;
  struct sockaddr hostaddr;
  bool buff_on_device;
  bool src_on_device;
};

static volatile int keep_running = 1;

void sigint_handler(int dummy) { keep_running = 0; }

/****************************************************************************************
 * Open socket connection on the client side, try to connect to the server by
 *the given IP address (servername). If success, return the connected socket
 *file descriptor ID Return value: socket fd - success, -1 - error
 ****************************************************************************************/
static int open_client_socket(const char *servername, int port) {
  struct addrinfo *res, *t;
  struct addrinfo hints = {.ai_family = AF_UNSPEC, .ai_socktype = SOCK_STREAM};
  char *service;
  int ret_val;
  int sockfd;

  if (asprintf(&service, "%d", port) < 0) return -1;

  ret_val = getaddrinfo(servername, service, &hints, &res);

  if (ret_val < 0) {
    fprintf(stderr, "FAILURE: %s for %s:%d\n", gai_strerror(ret_val),
            servername, port);
    free(service);
    return -1;
  }

  for (t = res; t; t = t->ai_next) {
    sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (sockfd >= 0) {
      if (!connect(sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(sockfd);
      sockfd = -1;
    }
  }

  freeaddrinfo(res);
  free(service);

  if (sockfd < 0) {
    // fprintf(stderr, "FAILURE: Couldn't connect to %s:%d\n", servername,
    // port);
    return -1;
  }

  return sockfd;
}

/****************************************************************************************
 * Open temporary socket connection on the server side, listening to the client.
 * Accepting connection from the client and closing temporary socket.
 * If success, return the accepted socket file descriptor ID
 * Return value: socket fd - success, -1 - error
 ****************************************************************************************/
static int open_server_socket(int port) {
  struct addrinfo *res, *t;
  struct addrinfo hints = {.ai_flags = AI_PASSIVE,
                           .ai_family = AF_UNSPEC,
                           .ai_socktype = SOCK_STREAM};
  char *service;
  int ret_val;
  int sockfd;
  int tmp_sockfd = -1;

  ret_val = asprintf(&service, "%d", port);
  if (ret_val < 0) return -1;

  ret_val = getaddrinfo(NULL, service, &hints, &res);
  if (ret_val < 0) {
    fprintf(stderr, "%s for port %d\n", gai_strerror(ret_val), port);
    free(service);
    return -1;
  }

  for (t = res; t; t = t->ai_next) {
    tmp_sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
    if (tmp_sockfd >= 0) {
      int optval = 1;

      setsockopt(tmp_sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof optval);

      if (!bind(tmp_sockfd, t->ai_addr, t->ai_addrlen)) break;
      close(tmp_sockfd);
      tmp_sockfd = -1;
    }
  }

  freeaddrinfo(res);
  free(service);

  if (tmp_sockfd < 0) {
    fprintf(stderr, "Couldn't listen to port %d\n", port);
    return -1;
  }

  listen(tmp_sockfd, 1);
  sockfd = accept(tmp_sockfd, NULL, 0);
  // close(tmp_sockfd);
  if (sockfd < 0) {
    fprintf(stderr, "accept() failed\n");
    return -1;
  }

  return sockfd;
}

static void usage(const char *argv0) {
  printf("Usage:\n");
  printf("  %s            start a server and wait for connection\n", argv0);
  printf("\n");
  printf("Options:\n");
  printf(
      "  -P, --persistent          server waits for additional client "
      "connections after tranfer is completed\n");
  printf(
      "  -a, --addr=<ipaddr>       ip address of the local host net device "
      "<ipaddr v4> (mandatory)\n");
  printf(
      "  -p, --port=<port>         listen on/connect to port <port> (default "
      "18515)\n");
  printf(
      "  -s, --size=<size>         size of message to exchange (default "
      "4096)\n");
  printf("  -n, --iters=<iters>       number of exchanges (default 1000)\n");
  printf(
      "  -l, --sg_list-len=<length> number of sge-s to send in sg_list "
      "(default 0 - old mode)\n");
  printf(
      "  -D, --debug-mask=<mask>   debug bitmask: bit 0 - debug print enable,\n"
      "                                           bit 1 - fast path debug "
      "print enable\n");
}

static int parse_command_line(int argc, char *argv[],
                              struct user_params *usr_par) {
  memset(usr_par, 0, sizeof *usr_par);
  /*Set defaults*/
  usr_par->port = 18515;
  usr_par->size = 4096;
  usr_par->iters = 1000;
  usr_par->buff_on_device = false;
  usr_par->src_on_device = false;

  while (1) {
    int c;

    static struct option long_options[] = {
        {.name = "task-flags", .has_arg = 1, .val = 't'},
        {.name = "persistent", .has_arg = 0, .val = 'P'},
        {.name = "addr", .has_arg = 1, .val = 'a'},
        {.name = "port", .has_arg = 1, .val = 'p'},
        {.name = "size", .has_arg = 1, .val = 's'},
        {.name = "iters", .has_arg = 1, .val = 'n'},
        {.name = "sg_list-len", .has_arg = 1, .val = 'l'},
        {.name = "debug-mask", .has_arg = 1, .val = 'D'},
        {.name = "buffer-device", .has_arg = 1, .val = 'b'},
        {.name = "src-device", .has_arg = 1, .val = 'd'},
        {0}};

    c = getopt_long(argc, argv, "t:Pa:p:s:n:l:D:b:d:", long_options, NULL);

    if (c == -1) break;

    switch (c) {
      case 't':
        usr_par->task = (strtol(optarg, NULL, 0) >> 0) & 1; /*bit 0*/
        break;

      case 'P':
        usr_par->persistent = 1;
        break;

      case 'a':
        get_addr(optarg, (struct sockaddr *)&usr_par->hostaddr);
        break;

      case 'p':
        usr_par->port = strtol(optarg, NULL, 0);
        if (usr_par->port < 0 || usr_par->port > 65535) {
          usage(argv[0]);
          return 1;
        }
        break;

      case 's':
        usr_par->size = strtol(optarg, NULL, 0);
        break;

      case 'n':
        usr_par->iters = strtol(optarg, NULL, 0);
        break;

      case 'l':
        usr_par->num_sges = strtol(optarg, NULL, 0);
        break;

      case 'D':
        debug = (strtol(optarg, NULL, 0) >> 0) & 1;           /*bit 0*/
        debug_fast_path = (strtol(optarg, NULL, 0) >> 1) & 1; /*bit 1*/
        break;
      case 'b':
        usr_par->buff_on_device = (strtol(optarg, NULL, 0) >> 0) & 1; /*bit 0*/
        break;
      case 'd':
        usr_par->src_on_device = (strtol(optarg, NULL, 0) >> 0) & 1; /*bit 0*/
        break;

      default:
        usage(argv[0]);
        return 1;
    }
  }

  if (optind < argc) {
    usage(argv[0]);
    return 1;
  }

  return 0;
}

// bool buff_on_device = false;
// bool src_on_device = true;

#define TIMING(cnt, cmd)                                                       \
  {                                                                            \
    struct timeval start, end;                                                 \
    gettimeofday(&start, NULL);                                                \
    cmd gettimeofday(&end, NULL);                                              \
    cnt +=                                                                     \
        (end.tv_sec - start.tv_sec) * 1000000 + (end.tv_usec - start.tv_usec); \
  }

int main(int argc, char *argv[]) {
  struct rdma_device *rdma_dev;
  struct timeval start;
  int cnt = 0;
  struct user_params usr_par;
  int ret_val = 0;
  int sockfd;
  struct iovec buf_iovec[MAX_SGES];
  int64_t pcie_cnt = 0;

  srand48(getpid() * time(NULL));

  ret_val = parse_command_line(argc, argv, &usr_par);
  if (ret_val) {
    return ret_val;
  }

  rdma_dev = rdma_open_device_server(&usr_par.hostaddr);
  if (!rdma_dev) {
    ret_val = 1;
    return ret_val;
  }

  /* Local memory buffer allocation */
  /* On the server side, we allocate buffer on CPU and not on GPU */
  void *buff = work_buffer_alloc(usr_par.size, 0, NULL);
  void *buff_d = work_buffer_alloc(usr_par.size, 1, "81:00.0");
  if (!buff) {
    ret_val = 1;
    goto clean_device;
  }

  /* RDMA buffer registration */
  struct rdma_buffer *rdma_buff;
  if (!usr_par.task) {
    for (size_t i = 0; i < usr_par.size; ++i) ((char *)buff)[i] = i;
    memcpyh2d(buff_d, buff, usr_par.size);
  }
  rdma_buff = rdma_buffer_reg(rdma_dev, usr_par.buff_on_device ? buff_d : buff,
                              usr_par.size);
  if (!rdma_buff) {
    ret_val = 1;
    goto clean_mem_buff;
  }

  struct sigaction act;
  act.sa_handler = sigint_handler;
  sigaction(SIGINT, &act, NULL);

sock_listen:
  printf("Listening to remote client...\n");
  sockfd = open_server_socket(usr_par.port);
  printf("get sockfd 1 \n");
  if (sockfd < 0) {
    goto clean_rdma_buff;
  }
  printf("Connection accepted.\n");
  // write(sockfd, ACK_MSG, sizeof(ACK_MSG));
  char ackmsg[sizeof ACK_MSG];
//   recv(sockfd, ackmsg, sizeof ackmsg, MSG_WAITALL);

  char desc_str[sizeof "0102030405060708:01020304:01020304:0102:010203:1:0102030405060708090a0b0c0d0e0f10"];
  struct rdma_task_attr task_attr;
  int i;
  int r_size;
  uint32_t flags; /* Use enum rdma_task_attr_flags */
  // payload attrs
  uint8_t pl_type;
  uint16_t pl_size;
  // int     expected_comp_events = usr_par.num_sges?
  // (usr_par.num_sges+MAX_SEND_SGE-1)/MAX_SEND_SGE: 1;

  for (i = 0; i < PACKAGE_TYPES; i++) {
    r_size = recv(sockfd, &pl_type, sizeof(pl_type), MSG_WAITALL);
    r_size = recv(sockfd, &pl_size, sizeof(pl_size), MSG_WAITALL);
    switch (pl_type) {
      case 0:  // RDMA_BUF_DESC
        /* Receiving RDMA data (address, size, rkey etc.) from socket as a
         * triger to start RDMA Read/Write operation */
        DEBUG_LOG_FAST_PATH(
            "Iteration %d: Waiting to Receive message of size %lu\n", cnt,
            sizeof desc_str);
        r_size = recv(sockfd, desc_str, pl_size * sizeof(char), MSG_WAITALL);
        if (r_size != sizeof desc_str) {
          fprintf(stderr,
                  "FAILURE: Couldn't receive RDMA data for iteration %d "
                  "(errno=%d '%m')\n",
                  cnt, errno);
          ret_val = 1;
          goto clean_socket;
        }
        break;
      case 1:  // TASK_ATTRS
          /* Receiving rw attr flags */;
        int s = pl_size * sizeof(char);
        char t[16];
        r_size = recv(sockfd, &t, s, MSG_WAITALL);
        if (r_size != s) {
          fprintf(stderr,
                  "FAILURE: Couldn't receive RDMA data for iteration %d "
                  "(errno=%d '%m')\n",
                  cnt, errno);
          ret_val = 1;
          goto clean_socket;
        }
        sscanf(t, "%08x", &flags);
        break;
    }
  }

  DEBUG_LOG_FAST_PATH("Received message \"%s\"\n", desc_str);
  memset(&task_attr, 0, sizeof task_attr);
  task_attr.remote_buf_desc_str = desc_str;
  task_attr.remote_buf_desc_length = sizeof desc_str;
  task_attr.local_buf_rdma = rdma_buff;
  task_attr.flags = flags;
  task_attr.wr_id = cnt;  // * expected_comp_events;

  /* Executing RDMA read */
  SDEBUG_LOG_FAST_PATH((char *)buff, "Read iteration N %d", cnt);
  /* Prepare send sg_list */
  if (usr_par.num_sges) {
    if (usr_par.num_sges > MAX_SGES) {
      fprintf(stderr, "WARN: num_sges %d is too big (max=%d)\n",
              usr_par.num_sges, MAX_SGES);
      ret_val = 1;
      goto clean_socket;
    }
    memset(buf_iovec, 0, sizeof buf_iovec);
    task_attr.local_buf_iovcnt = usr_par.num_sges;
    task_attr.local_buf_iovec = buf_iovec;

    size_t portion_size;
    portion_size =
        (usr_par.size / usr_par.num_sges) & 0xFFFFFFC0; /* 64 byte aligned */
    for (i = 0; i < usr_par.num_sges; i++) {
      buf_iovec[i].iov_base = buff + (i * portion_size);
      buf_iovec[i].iov_len = portion_size;
    }
  }

  if (gettimeofday(&start, NULL)) {
    perror("gettimeofday");
    ret_val = 1;
    goto clean_socket;
  }

  /****************************************************************************************************
   * The main loop where we client and server send and receive "iters" number of
   * messages
   */
  for (cnt = 0; cnt < usr_par.iters && keep_running; cnt++) {
    // send
    if (!usr_par.task) {
      TIMING(pcie_cnt, if (usr_par.src_on_device && !usr_par.buff_on_device)
                           memcpyd2h(buff, buff_d, usr_par.size);)
    }
    recv(sockfd, ackmsg, sizeof ackmsg, MSG_WAITALL);
    int r_size;
    ret_val = rdma_submit_task(&task_attr);
    if (ret_val) {
      goto clean_socket;
    }

    /* Completion queue polling loop */
    DEBUG_LOG_FAST_PATH("Polling completion queue\n");
    struct rdma_completion_event rdma_comp_ev[10];
    int reported_ev = 0;
    do {
      reported_ev +=
          rdma_poll_completions(rdma_dev, &rdma_comp_ev[reported_ev],
                                10 /*expected_comp_events-reported_ev*/);
      // TODO - we can put sleep here
    } while (reported_ev < 1 && keep_running /*expected_comp_events*/);
    DEBUG_LOG_FAST_PATH("Finished polling\n");

    for (i = 0; i < reported_ev; ++i) {
      if (rdma_comp_ev[i].status != IBV_WC_SUCCESS) {
        fprintf(stderr, "FAILURE: status \"%s\" (%d) for wr_id %d\n",
                ibv_wc_status_str(rdma_comp_ev[i].status),
                rdma_comp_ev[i].status, (int)rdma_comp_ev[i].wr_id);
        ret_val = 1;
        if (usr_par.persistent && keep_running) {
          rdma_reset_device(rdma_dev);
        }
        goto clean_socket;
      }
    }

    // Sending ack-message to the client, confirming that RDMA read/write has
    // been completet
    if (write(sockfd, ACK_MSG, sizeof(ACK_MSG)) != sizeof(ACK_MSG)) {
      fprintf(stderr, "FAILURE: Couldn't send \"%c\" msg (errno=%d '%m')\n",
              ACK_MSG, errno);
      ret_val = 1;
      goto clean_socket;
    }
    // recv
    if (usr_par.task) {
      TIMING(pcie_cnt, if (usr_par.src_on_device && !usr_par.buff_on_device)
                           memcpyh2d(buff_d, buff, usr_par.size);)
    }
  }
  /****************************************************************************************************/

  float total_cnt;
  printf("dst_on_device=%d buffer_on_device=%d\n", usr_par.src_on_device,
         usr_par.buff_on_device);
  total_cnt = print_run_time(start, usr_par.size, usr_par.iters);
  printf("pcie: %.2f %\n", 1.0 * pcie_cnt / total_cnt * 100);
  printf("rdma BW=%.2f Gbps\n",
         usr_par.size * usr_par.iters * 1. / (total_cnt - pcie_cnt) / 1000 * 8);
  if (ret_val) {
    goto clean_socket;
  }
  if (usr_par.task) {
    if (usr_par.src_on_device) memcpyd2h(buff, buff_d, usr_par.size);
    for (size_t i = 0; i < usr_par.size; ++i)
      if (((char *)buff)[i] != (char)(i)) {
        printf("%ld: %d, %d\n", i, ((char *)buff)[i], (char)(i));
        return 1;
      }
  }

clean_socket:
  close(sockfd);
  if (usr_par.persistent && keep_running) goto sock_listen;

clean_rdma_buff:
  rdma_buffer_dereg(rdma_buff);

clean_mem_buff:
  work_buffer_free(buff, 0);
  work_buffer_free(buff_d, 1);

clean_device:
  rdma_close_device(rdma_dev);

  return ret_val;
}
