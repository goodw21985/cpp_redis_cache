// MIT License
//
// Copyright (c) 2016-2017 Simon Ninon <simon.ninon@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

#include <tacopie/network/io_service.hpp>
#include <tacopie/utils/error.hpp>
#include <tacopie/utils/logger.hpp>

#include <fcntl.h>

#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif /* _WIN32 */

namespace tacopie {

//!
//! default io_service getter & setter
//!

static std::shared_ptr<io_service> io_service_default_instance = nullptr;

const std::shared_ptr<io_service>&
get_default_io_service(void) {
  if (io_service_default_instance == nullptr) {
    io_service_default_instance = std::make_shared<io_service>();
  }

  return io_service_default_instance;
}

void
set_default_io_service(const std::shared_ptr<io_service>& service) {
  __TACOPIE_LOG(debug, "setting new default_io_service");
  io_service_default_instance = service;
}

//!
//! ctor & dtor
//!

io_service::io_service(void)
#ifdef _WIN32
: m_should_stop(ATOMIC_VAR_INIT(false))
#else
: m_should_stop(false)
#endif /* _WIN32 */
{
  __TACOPIE_LOG(debug, "create io_service");

  //! Start worker after everything has been initialized
  m_poll_worker = std::thread(std::bind(&io_service::poll, this));
}

io_service::~io_service(void) {
  __TACOPIE_LOG(debug, "destroy io_service");

  m_should_stop = true;

  m_notifier.notify();
  if (m_poll_worker.joinable()) {
    m_poll_worker.join();
  }
}

//!
//! poll worker function
//!

void
io_service::poll(void) {
  __TACOPIE_LOG(debug, "starting poll() worker");

  while (!m_should_stop) {
    int ndfs = init_poll_fds_info();

    //! setup timeout
    struct timeval* timeout_ptr = NULL;
#ifdef __TACOPIE_TIMEOUT
    struct timeval timeout;
    timeout.tv_usec = __TACOPIE_TIMEOUT;
    timeout_ptr     = &timeout;
#endif /* __TACOPIE_TIMEOUT */

    __TACOPIE_LOG(debug, "polling fds");
    if (select(ndfs, &m_rd_set, &m_wr_set, NULL, timeout_ptr) > 0) {
      process_events();
    }
    else {
      __TACOPIE_LOG(debug, "poll woke up, but nothing to process");
    }
  }

  __TACOPIE_LOG(debug, "stop poll() worker");
}

//!
//! process poll detected events
//!

void
io_service::process_events(void) {
  auto callbacks = get_callbacks_to_execute();

  for (const auto& callback : callbacks) {
    callback();
  }
}

std::vector<std::function<void()>>
io_service::get_callbacks_to_execute(void) {
  std::lock_guard<std::mutex> lock(m_tracked_sockets_mtx);
  std::vector<std::function<void()>> callbacks;

  __TACOPIE_LOG(debug, "processing events");

  for (const auto& fd : m_polled_fds) {
    if (fd == m_notifier.get_read_fd() && FD_ISSET(fd, &m_rd_set)) {
      m_notifier.clr_buffer();
      continue;
    }

    auto it = m_tracked_sockets.find(fd);
    if (it != m_tracked_sockets.end()) {
      const auto& socket      = it->second;
      const auto& rd_callback = socket.rd_callback;
      const auto& wr_callback = socket.wr_callback;

      if (FD_ISSET(fd, &m_rd_set) && rd_callback) {
        callbacks.push_back(std::bind(rd_callback, fd));
      }
      if (FD_ISSET(fd, &m_wr_set) && wr_callback) {
        callbacks.push_back(std::bind(wr_callback, fd));
      }
    }
  }

  return callbacks;
}

//!
//! init m_poll_fds_info
//!

int
io_service::init_poll_fds_info(void) {
  std::lock_guard<std::mutex> lock(m_tracked_sockets_mtx);

  m_polled_fds.clear();
  FD_ZERO(&m_rd_set);
  FD_ZERO(&m_wr_set);

  int ndfs = (int) m_notifier.get_read_fd();
  FD_SET(m_notifier.get_read_fd(), &m_rd_set);
  m_polled_fds.push_back(m_notifier.get_read_fd());

  for (const auto& socket : m_tracked_sockets) {
    const auto& fd          = socket.first;
    const auto& socket_info = socket.second;
    bool should_rd          = socket_info.rd_callback != nullptr;
    bool should_wr          = socket_info.wr_callback != nullptr;

    if (should_rd) {
      FD_SET(fd, &m_rd_set);
    }

    if (should_wr) {
      FD_SET(fd, &m_wr_set);
    }

    if (should_rd || should_wr) {
      m_polled_fds.push_back(fd);

      if ((int) fd > ndfs) {
        ndfs = (int) fd;
      }
    }
  }

  return ndfs + 1;
}

//!
//! track & untrack socket
//!

void
io_service::track(const tcp_socket& socket, const event_callback_t& rd_callback, const event_callback_t& wr_callback) {
  std::lock_guard<std::mutex> lock(m_tracked_sockets_mtx);

  __TACOPIE_LOG(debug, "track new socket");

  auto& track_info       = m_tracked_sockets[socket.get_fd()];
  track_info.rd_callback = rd_callback;
  track_info.wr_callback = wr_callback;

  m_notifier.notify();
}

void
io_service::set_rd_callback(const tcp_socket& socket, const event_callback_t& event_callback) {
  std::lock_guard<std::mutex> lock(m_tracked_sockets_mtx);

  __TACOPIE_LOG(debug, "update read socket tracking callback");

  auto& track_info       = m_tracked_sockets[socket.get_fd()];
  track_info.rd_callback = event_callback;

  m_notifier.notify();
}

void
io_service::set_wr_callback(const tcp_socket& socket, const event_callback_t& event_callback) {
  std::lock_guard<std::mutex> lock(m_tracked_sockets_mtx);

  __TACOPIE_LOG(debug, "update write socket tracking callback");

  auto& track_info       = m_tracked_sockets[socket.get_fd()];
  track_info.wr_callback = event_callback;

  m_notifier.notify();
}

void
io_service::untrack(const tcp_socket& socket) {
  std::lock_guard<std::mutex> lock(m_tracked_sockets_mtx);

  auto it = m_tracked_sockets.find(socket.get_fd());
  if (it != m_tracked_sockets.end()) {
    __TACOPIE_LOG(debug, "untrack socket");
    m_tracked_sockets.erase(it);
  }

  m_notifier.notify();
}

} // namespace tacopie
