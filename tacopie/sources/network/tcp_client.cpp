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

#include <tacopie/network/tcp_client.hpp>
#include <tacopie/utils/error.hpp>
#include <tacopie/utils/logger.hpp>

namespace tacopie {

//!
//! ctor & dtor
//!

tcp_client::tcp_client(void)
: m_io_service(get_default_io_service())
, m_thread_pool(utils::get_default_thread_pool())
, m_disconnection_handler(nullptr) {
  __TACOPIE_LOG(debug, "create tcp_client");
}

tcp_client::~tcp_client(void) {
  __TACOPIE_LOG(debug, "destroy tcp_client");
  disconnect();

  //! prevent execution of additional disconnection callbacks
  m_disconnection_handler = nullptr;

  //! prevent the client to be destroyed before all tasks completed
  std::mutex dtor_mtx;
  std::unique_lock<std::mutex> lock(dtor_mtx);
  m_pending_tasks_condvar.wait(lock, [=] { return m_pending_tasks == 0; });
}

//!
//! custom ctor
//! build client from existing socket
//!

tcp_client::tcp_client(tcp_socket&& socket)
: m_io_service(get_default_io_service())
, m_thread_pool(utils::get_default_thread_pool())
, m_socket(std::move(socket))
, m_disconnection_handler(nullptr) {
  m_is_connected = true;
  __TACOPIE_LOG(debug, "create tcp_client");
  m_io_service->track(m_socket);
}

//!
//! get host & port information
//!

const std::string&
tcp_client::get_host(void) const {
  return m_socket.get_host();
}

std::uint32_t
tcp_client::get_port(void) const {
  return m_socket.get_port();
}

//!
//! start & stop the tcp client
//!

void
tcp_client::connect(const std::string& host, std::uint32_t port, std::uint32_t timeout_msecs) {
  if (is_connected()) { __TACOPIE_THROW(warn, "tcp_client is already connected"); }

  try {
    m_socket.connect(host, port, timeout_msecs);
    m_io_service->track(m_socket);
  }
  catch (const tacopie_error& e) {
    m_socket.close();
    throw e;
  }

  m_is_connected = true;

  __TACOPIE_LOG(info, "tcp_client connected");
}

void
tcp_client::disconnect(void) {
  if (!is_connected()) { return; }

  //! update state
  m_is_connected = false;

  //! clear all pending requests
  clear_read_requests();
  clear_write_requests();

  //! remove socket from io service
  m_io_service->untrack(m_socket);

  //! close the socket
  m_socket.close();

  __TACOPIE_LOG(info, "tcp_client disconnected");
}

//!
//! Clear pending requests
//!
void
tcp_client::clear_read_requests(void) {
  std::lock_guard<std::mutex> lock(m_read_requests_mtx);

  std::queue<read_request> empty;
  std::swap(m_read_requests, empty);
}

void
tcp_client::clear_write_requests(void) {
  std::lock_guard<std::mutex> lock(m_write_requests_mtx);

  std::queue<write_request> empty;
  std::swap(m_write_requests, empty);
}

//!
//! io service read callback
//!

void
tcp_client::on_read_available(fd_t) {
  __TACOPIE_LOG(info, "read available");

  std::shared_ptr<read_result> result = std::make_shared<read_result>();
  auto callback                       = process_read(*result);

  if (!result->success) {
    __TACOPIE_LOG(warn, "read operation failure");
    disconnect();
  }

  //! keep track that a new async task is being executed
  ++m_pending_tasks;

  m_thread_pool->add_task([=] {
    if (callback) {
      callback(*result);
    }

    if (!result->success && m_disconnection_handler) {
      m_disconnection_handler();
    }

    //! notify about task completion
    --m_pending_tasks;
    m_pending_tasks_condvar.notify_one();
  });
}

//!
//! io service write callback
//!

void
tcp_client::on_write_available(fd_t) {
  __TACOPIE_LOG(info, "write available");

  std::shared_ptr<write_result> result = std::make_shared<write_result>();
  auto callback                        = process_write(*result);

  if (!result->success) {
    __TACOPIE_LOG(warn, "write operation failure");
    disconnect();
  }

  //! keep track that a new async task is being executed
  ++m_pending_tasks;

  m_thread_pool->add_task([=] {
    if (callback) {
      callback(*result);
    }

    if (!result->success && m_disconnection_handler) {
      m_disconnection_handler();
    }

    //! notify about task completion
    --m_pending_tasks;
    m_pending_tasks_condvar.notify_one();
  });
}

//!
//! process read & write operations when available
//!

tcp_client::async_read_callback_t
tcp_client::process_read(read_result& result) {
  std::lock_guard<std::mutex> lock(m_read_requests_mtx);

  if (m_read_requests.empty()) { return nullptr; }

  const auto& request = m_read_requests.front();
  auto callback       = request.async_read_callback;

  try {
    result.buffer  = m_socket.recv(request.size);
    result.success = true;
  }
  catch (const tacopie::tacopie_error&) {
    result.success = false;
  }

  m_read_requests.pop();

  if (m_read_requests.empty()) {
    m_io_service->set_rd_callback(m_socket, nullptr);
  }

  return callback;
}

tcp_client::async_write_callback_t
tcp_client::process_write(write_result& result) {
  std::lock_guard<std::mutex> lock(m_write_requests_mtx);

  if (m_write_requests.empty()) { return nullptr; }

  const auto& request = m_write_requests.front();
  auto callback       = request.async_write_callback;

  try {
    result.size    = m_socket.send(request.buffer, request.buffer.size());
    result.success = true;
  }
  catch (const tacopie::tacopie_error&) {
    result.success = false;
  }

  m_write_requests.pop();

  if (m_write_requests.empty()) {
    m_io_service->set_wr_callback(m_socket, nullptr);
  }

  return callback;
}

//!
//! async read & write operations
//!

void
tcp_client::async_read(const read_request& request) {
  std::lock_guard<std::mutex> lock(m_read_requests_mtx);

  if (is_connected()) {
    m_io_service->set_rd_callback(m_socket, std::bind(&tcp_client::on_read_available, this, std::placeholders::_1));
    m_read_requests.push(request);
  }
  else {
    __TACOPIE_THROW(warn, "tcp_client is disconnected");
  }
}

void
tcp_client::async_write(const write_request& request) {
  std::lock_guard<std::mutex> lock(m_write_requests_mtx);

  if (is_connected()) {
    m_io_service->set_wr_callback(m_socket, std::bind(&tcp_client::on_write_available, this, std::placeholders::_1));
    m_write_requests.push(request);
  }
  else {
    __TACOPIE_THROW(warn, "tcp_client is disconnected");
  }
}

//!
//! socket getter
//!

tacopie::tcp_socket&
tcp_client::get_socket(void) {
  return m_socket;
}

const tacopie::tcp_socket&
tcp_client::get_socket(void) const {
  return m_socket;
}

//!
//! io_service getter & setter
//!
const std::shared_ptr<io_service>&
tcp_client::get_io_service(void) const {
  return m_io_service;
}

void
tcp_client::set_io_service(const std::shared_ptr<io_service>& io_service) {
  m_io_service = io_service;
}

//!
//! thread_pool getter & setter
//!
const std::shared_ptr<utils::thread_pool>&
tcp_client::get_thread_pool(void) const {
  return m_thread_pool;
}

void
tcp_client::set_thread_pool(const std::shared_ptr<utils::thread_pool>& thread_pool) {
  m_thread_pool = thread_pool;
}

//!
//! set on disconnection handler
//!

void
tcp_client::set_on_disconnection_handler(const disconnection_handler_t& disconnection_handler) {
  m_disconnection_handler = disconnection_handler;
}

//!
//! returns whether the client is currently running or not
//!

bool
tcp_client::is_connected(void) const {
  return m_is_connected;
}

//!
//! comparison operator
//!
bool
tcp_client::operator==(const tcp_client& rhs) const {
  return m_socket == rhs.m_socket;
}

bool
tcp_client::operator!=(const tcp_client& rhs) const {
  return !operator==(rhs);
}

} // namespace tacopie
