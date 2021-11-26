#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define CRIT_SEC_BEG
#define CRIT_SEC_END

static const ssize_t MAX_BUF_SZ = 128 * 1024;

static struct timeval timeout = {
    .tv_sec = 60,
    .tv_usec = 0,
};

struct conn {
  int rfd;
  int child_wfd;
  int child_rfd;
  int wfd;
  char *buf;
  size_t buf_sz;
  ssize_t bytes_read;
  ssize_t bytes_written;
  bool needs_flush;
  bool finished_reading;
};

int
main(int argc, const char *const argv[]) {
  --argc;
  ++argv;

  if (argc != 2) {
    printf("USAGE: n-pipes <n> <path_to_file>\n");
    return EXIT_FAILURE;
  }

  char *end;
  errno = 0;
  long n_children = strtol(argv[0], &end, 10);
  if (errno != 0) {
    printf("USAGE: n-pipes <n> <path_to_file>\n");
    return EXIT_FAILURE;
  }
  ++argv;

  const char *file_name = argv[0];
  int file_fd = open(file_name, O_RDONLY);
  if (file_fd == -1) {
    perror("SYSTEM ERROR: open failed");
    return EXIT_FAILURE;
  }

  struct conn *conns = calloc(n_children, sizeof(*conns));
  if (conns == NULL) {
    perror("SYSTEM ERROR: calloc failed");
    return EXIT_FAILURE;
  }

  for (size_t i = 0; i < n_children; ++i) {
    int pfds[2];
    int rc = pipe(pfds);
    if (rc != 0) {
      perror("SYSTEM ERROR: pipe failed");
      return EXIT_FAILURE;
    }
    conns[i].child_rfd = pfds[0];
    conns[i].wfd = pfds[1];
    rc = pipe(pfds);
    if (rc != 0) {
      perror("SYSTEM ERROR: pipe failed");
      return EXIT_FAILURE;
    }
    conns[i].rfd = pfds[0];
    conns[i].child_wfd = pfds[1];
    conns[i].buf_sz = (size_t) fmin(pow(3, (double) (n_children - i + 4)),
                                    (double) MAX_BUF_SZ - 1);
    char *buf = calloc(conns[i].buf_sz, sizeof(*buf));
    if (buf == NULL) {
      perror("SYSTEM ERROR: calloc failed");
      return EXIT_FAILURE;
    }
    conns[i].buf = buf;
  }
  int rc = close(conns[0].child_rfd);
  if (rc != 0) {
    perror("SYSTEM ERROR: close failed");
    return EXIT_FAILURE;
  }
  conns[0].child_rfd = file_fd;
  rc = close(conns[0].wfd);
  if (rc != 0) {
    perror("SYSTEM ERROR: close failed");
  }

  for (size_t i = 0; i < n_children; ++i) {
    int rfd = conns[i].child_rfd;
    int wfd = conns[i].child_wfd;
    char *buf = conns[i].buf;
    size_t buf_sz = conns[i].buf_sz;
    pid_t child_pid = fork();
    if (child_pid == -1) {
      perror("SYSTEM ERROR: fork failed");
      return EXIT_FAILURE;
    }
    if (child_pid != 0) {
      continue;
    }

    if (i != 0) {
      rc = close(conns[0].rfd);
      if (rc) {
        perror("SYSTEM ERROR: close failed");
        return EXIT_FAILURE;
      }
      rc = close(conns[0].child_wfd);
      if (rc) {
        perror("SYSTEM ERROR: close failed");
        return EXIT_FAILURE;
      }
    }

    for (size_t j = 1; j < n_children; ++j) {
      rc = close(conns[j].wfd);
      if (rc != 0) {
        perror("SYSTEM ERROR: close failed");
        return EXIT_FAILURE;
      }
      rc = close(conns[j].rfd);
      if (rc != 0) {
        perror("SYSTEM ERROR: close failed");
        return EXIT_FAILURE;
      }

      if (j == i) {
        continue;
      }

      rc = close(conns[j].child_rfd);
      if (rc != 0) {
        perror("SYSTEM ERROR: close failed");
        return EXIT_FAILURE;
      }
      rc = close(conns[j].child_wfd);
      if (rc != 0) {
        perror("SYSTEM ERROR: close failed");
        return EXIT_FAILURE;
      }
    }

    while (true) {
      ssize_t bytes_read = read(rfd, buf, buf_sz);
      if (bytes_read < 0) {
        fprintf(stderr, "child %zu: ", i);
        perror("SYSTEM ERROR: read failed");
        return EXIT_FAILURE;
      }
      if (bytes_read == 0) {
        return EXIT_SUCCESS;
      }

      CRIT_SEC_BEG
      ssize_t bytes_written = write(wfd, buf, bytes_read);
      if (bytes_written < 0) {
        perror("SYSTEM ERROR: write failed");
        return EXIT_FAILURE;
      }
      assert(bytes_written == bytes_read);
      CRIT_SEC_END
    }
  }

  rc = close(conns[0].child_wfd);
  if (rc) {
    perror("SYSTEM ERROR: close failed");
    return EXIT_FAILURE;
  }
  rc = fcntl(conns[0].rfd, F_SETFL, O_NONBLOCK);
  if (rc) {
    perror("SYSTEM ERROR: fcntl failed");
    return EXIT_FAILURE;
  }
  for (size_t i = 1; i < n_children; ++i) {
    rc = fcntl(conns[i].rfd, F_SETFL, O_NONBLOCK);
    if (rc) {
      perror("SYSTEM ERROR: fcntl failed");
      return EXIT_FAILURE;
    }
    rc = fcntl(conns[i].wfd, F_SETFL, O_NONBLOCK);
    if (rc) {
      perror("SYSTEM ERROR: fcntl failed");
      return EXIT_FAILURE;
    }
    rc = close(conns[i].child_rfd);
    if (rc != 0) {
      perror("SYSTEM ERROR: close failed");
      return EXIT_FAILURE;
    }
    rc = close(conns[i].child_wfd);
    if (rc != 0) {
      perror("SYSTEM ERROR: close failed");
      return EXIT_FAILURE;
    }
  }

  fd_set rfds;
  FD_ZERO(&rfds);
  int max_rfd = -1;
  fd_set wfds;
  FD_ZERO(&wfds);
  int max_wfd = -1;
  int rfd = conns[0].rfd;
  if (max_rfd < rfd) {
    max_rfd = rfd;
  }
  FD_SET(rfd, &rfds);
  int wfd;
  for (size_t i = 1; i < n_children; ++i) {
    rfd = conns[i].rfd;
    FD_SET(rfd, &rfds);
    if (max_rfd < rfd) {
      max_rfd = rfd;
    }
    wfd = conns[i].wfd;
    FD_SET(wfd, &wfds);
    if (max_wfd < wfd) {
      max_wfd = wfd;
    }
  }
  int max_fd = (int) fmax(max_rfd, max_wfd);

  while (true) {
    fd_set dirty_rfds;
    memcpy(&dirty_rfds, &rfds, sizeof(rfds));
    fd_set dirty_wfds;
    memcpy(&dirty_wfds, &wfds, sizeof(wfds));

    int n_ready = select(max_fd + 1, &dirty_rfds, &dirty_wfds, NULL,
                         &timeout);
    if (n_ready == -1) {
      perror("SYSTEM ERROR: select failed");
      return EXIT_FAILURE;
    }
    if (n_ready == 0) {
      perror("CLIENT ERROR: child process communication timeout");
      return EXIT_FAILURE;
    }

    for (size_t i = 0; i < n_children && n_ready > 0; ++i) {
      rfd = conns[i].rfd;
      wfd = conns[i].wfd;
      size_t buf_sz = conns[i].buf_sz;

      if (FD_ISSET(rfd, &dirty_rfds)) {
        --n_ready;

        CRIT_SEC_BEG
        if (!conns[i].needs_flush) {
          ssize_t bytes_read = read(rfd, conns[i].buf, buf_sz);
          if (bytes_read < 0) {
            perror("SYSTEM ERROR: read failed");
            return EXIT_FAILURE;
          }
          if (bytes_read == 0) {
            if (i == n_children - 1) {
              return EXIT_SUCCESS;
            }

            rc = close(rfd);
            if (rc != 0) {
              perror("SYSTEM ERROR: close failed");
              return EXIT_FAILURE;
            }
            conns[i].finished_reading = true;
            FD_CLR(rfd, &rfds);
          }
          if (i == n_children - 1) {
            ssize_t bytes_written = write(STDOUT_FILENO,
                                          conns[i].buf,
                                          bytes_read);
            if (bytes_written < 0) {
              perror("SYSTEM ERROR: write failed");
              return EXIT_FAILURE;
            }
            assert(bytes_written == bytes_read);
          } else {
            conns[i].bytes_read = bytes_read;
            conns[i].needs_flush = !conns[i].finished_reading;
            FD_SET(conns[i + 1].wfd, &wfds);
          }
        } else {
          FD_CLR(rfd, &rfds);
        }
        CRIT_SEC_END
      }
      if (FD_ISSET(wfd, &dirty_wfds)) {
        --n_ready;

        CRIT_SEC_BEG
        if (conns[i - 1].needs_flush) {
          ssize_t bytes_written =
              write(wfd, conns[i - 1].buf + conns[i - 1].bytes_written,
                    conns[i - 1].bytes_read - conns[i - 1].bytes_written);
          if (bytes_written < 0) {
            perror("SYSTEM ERROR: write failed");
            return EXIT_FAILURE;
          }
          conns[i - 1].bytes_written += bytes_written;
          if (conns[i - 1].bytes_written == conns[i - 1].bytes_read) {
            conns[i - 1].bytes_written = 0;
            conns[i - 1].needs_flush = false;
            FD_SET(conns[i - 1].rfd, &rfds);
          }
        } else {
          FD_CLR(wfd, &wfds);
        }
        if (conns[i - 1].finished_reading) {
          rc = close(wfd);
          if (rc != 0) {
            perror("SYSTEM ERROR: close failed");
            return EXIT_FAILURE;
          }
          FD_CLR(wfd, &wfds);
        }
        CRIT_SEC_END
      }
    }
  }
}
