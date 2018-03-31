# Changelog


## [v4.0.0](https://github.com/Cylix/tacopie/releases/tag/4.0.0)
### Tag
`4.0.0`
### Date
March 11th, 2018
### Changes
* CMake variable `IO_SERVICE_NB_WORKERS` changed into `DEFAULT_THREAD_POOL_SIZE`.
* The `io_service` does not execute the callbacks in the background anymore. Instead, that's the role of the listener to spawn a callback if necessary. `tcp_client` will now read from or write to the socket synchronously and will call the `tcp_client` callbacks (from `async_read` and `async_write`) asynchronously by adding a new task to the thread pool.
* `io_service::untrack` now always effectively untrack a socket immediately (no more marking for untrack with the untracking done in the background as before).
* `tcp_client::disconnect` do not have the parameter `wait_for_removal` as `io_service::untrack` always untrack immediately.
* fix `__TACOPIE_CONNECTION_QUEUE_SIZE` was not using the CMake variable `CONNECTION_QUEUE_SIZE` value
* `io_service::stop` do not have the parameters `wait_for_removal` and `recursive_wait_for_removal` as `io_service::untrack` always untrack immediately.
* refactore the set_nb_workers in the io service (make it effectively work and less error prone).
### Additions
* `tcp_client::set_io_service` to set the io service of a tcp_client
* `tcp_client::get_thread_pool` to get the thread pool of a tcp_client
* `tcp_client::set_thread_pool` to set the thread pool of a tcp_client
* `tcp_server::set_io_service` to set the io service of a tcp_client
* `utils::get_default_thread_pool` to get the default thread pool used internally
* `utils::set_default_thread_pool` to set the default thread pool used internally
* add pragma comment to ensure MSVC links with ws2_32
* add guards in platform specific source files in case integrator want to simply grab all cpp files
### Removals
* `io_service::set_nb_workers` as the io_service does not execute callbacks asynchronously anymore (does not use the thread pool internally).
* `io_service::wait_for_removal` as `io_service::untrack` always untrack immediately.




## [v3.2.0](https://github.com/Cylix/tacopie/releases/tag/3.2.0)
### Tag
`3.2.0`
### Date
November 13th, 2017
### Changes
* fork support: allow set_default_io_service to take nullptr. In order to safely fork, call set_default_io_service(nullptr) to make sure the io_service destructor is called and all underlying threads joined.
* fix: timeout for connection not working due to invalid param to select, now working
* improvement: make sure socket is in blocking mode before connection (#32) as it differs from one OS to another
* improvement: check for non-blocking connect errors with getsockopt to avoid connect reporting a successful connection followed by a call to disconnection handler (now connect report a failed connection as expected).
### Additions
* ipv6 support (connect, bind and accept operations, on tcp_server and tcp_client)
### Removals
None




## [v3.1.0](https://github.com/Cylix/tacopie/releases/tag/3.1.0)
### Tag
`3.1.0`
### Date
November 2nd, 2017
### Changes
* Improvement: For windows, if port is 0, use the default AF_INET windows API behavior (bind to first port available). Behavior on unix is unchanged (is unix socket).
* CMake fix: Remove explicit STATIC in add_library call so dynamic libraries can be built with -DBUILD_SHARED_LIBS=ON
### Additions
* Visual Studio C++ solution
### Removals
None




## [v3.0.1](https://github.com/Cylix/tacopie/releases/tag/3.0.1)
### Tag
`3.0.1`
### Date
September 26th, 2017
### Changes
* Fix some compilation issues on windows
### Additions
None.
### Removals
* Private, Protected and Static functions from doxygen documentation





## [v3.0.0](https://github.com/Cylix/tacopie/releases/tag/3.0.0)
### Tag
`3.0.0`
### Date
September 20th, 2017
### Changes
* clear pending read and write requests on disconnection
* io_service access
* ability to modify number of io service worker at runtime
### Additions
* doxygen documentation
* connection timeout if requested (for `tcp_socket` and `tcp_client`)
* ability to change the number of `io_service` workers (or `thread_pool` threads) at runtime
### Removals
None.





## [v2.4.4](https://github.com/Cylix/tacopie/releases/tag/2.4.4)
### Tag
`2.4.4`
### Date
July 2nd, 2017
### Changes
* add calls to WSAStartup and WSACleanup in examples (#16).
* fix #17 and cpp_redis#85 (select keep sleeping and does not process incoming read/write events).
### Additions
None.
### Removals
None.





## [v2.4.3](https://github.com/Cylix/tacopie/releases/tag/2.4.3)
### Tag
`2.4.3`
### Date
June 19th, 2017
### Changes
* Remove unnecessary use of self-pipe to try to fix high-CPU usage issued reported on this repository and on cpp_redis repository.
### Additions
* CMake compilation flag `SELECT_TIMEOUT` that can be used to define the select timeout in nano seconds. By default, timeout is set to NULL (unlimited).
### Removals
None.





## [v2.4.2](https://github.com/Cylix/tacopie/releases/tag/2.4.2)
### Tag
`2.4.2`
### Date
June 11th, 2017
### Changes
* Compilation Fix
* change behavior of on_new_connection_handler. Returning true means connection is handled by tcp_client wrapper and nothing will be done by tcp_server. Returning false means connection is handled by tcp_server, will be stored in an internal list and tcp_client disconnection_handler overridden.
### Additions
* `get_host` & `get_port` methods for `tcp_client`
### Removals
None.





## [v2.4.1](https://github.com/Cylix/tacopie/releases/tag/2.4.1)
### Tag
`2.4.1`
### Date
April 30th, 2017
### Changes
* Compile project with `/bigobj` option on windows
* Fix behavior when trying to reconnect from disconnection_handler callback
### Additions
None.
### Removals
None.





## [v2.4.0](https://github.com/Cylix/tacopie/releases/tag/2.4.0)
### Tag
`2.4.0`
### Date
April 9th, 2017
### Changes
None.
### Additions
* Provide support for Unix socket. Simply pass in 0 as the port when building a `tcp_socket`, `tcp_client` or `tcp_server`. Then, the host will automatically be treated as the path to a Unix socket instead of a real host.
### Removals
None.





## [v2.3.0](https://github.com/Cylix/tacopie/releases/tag/2.3.0)
### Tag
`2.3.0`
### Date
April 9th, 2017
### Changes
None.
### Additions
* TCP server now supports `wait_for_removal` as a parameter for `.stop()`. Please refer to the [documentation](https://github.com/Cylix/tacopie/wiki/TCP-Server#stop) for more information.
### Removals
None.




## [v2.2.0](https://github.com/Cylix/tacopie/releases/tag/2.2.0)
### Tag
`2.2.0`
### Date
April 4th, 2017
### Changes
* IO Service is now based on `select` and not on `poll` anymore to solve some issues encountered on windows due to the buggy implementation of `poll` on windows Systems.
### Additions
None.
### Removals
None.




## [v2.1.0](https://github.com/Cylix/tacopie/releases/tag/2.1.0)
### Tag
`2.1.0`
### Date
March 19th, 2017
### Changes
* read and write TCP client callbacks now takes a reference to the result as parameter instead of a const-reference.
### Additions
None.
### Removals
* install_deps.sh has been removed in favor of CMakelists.txt enhancement.



## [v2.0.1](https://github.com/Cylix/tacopie/releases/tag/2.0.1)
### Tag
`2.0.1`
### Date
Feb. 17th, 2017
### Changes
* Fix: replace gethostbyname() (not thread-safe) usage by getaddrinfo() (thread-safe) on unix platforms. No change where required on windows as getaddrinfo() was already in use before.
### Additions
None.
### Removals
None.




## [v2.0.0](https://github.com/Cylix/tacopie/releases/tag/2.0.0)
### Tag
`2.0.0`
### Date
Jan. 29th, 2017
### Changes
* Fix: some sockets were not removed from io_service tracking. Now fixed
* Improvement: handle POLLHUP events
### Additions
* Feature: Port the library onto windows
* Feature: Make the library usable by cpp_redis
### Removals
None.




## [v1.1.0](https://github.com/Cylix/tacopie/releases/tag/1.1.0)
### Tag
`1.1.0`
### Date
Dec. 16th, 2016
### Changes
* Make server on_new_connection callback take shared_ptr as parameter instead of reference (provide more flexibility to the client app)
### Additions
* Provide access to tcp_socket in the public API of tcp_client and tcp_server
### Removals
None.




## [v1.0.0](https://github.com/Cylix/tacopie/releases/tag/1.0.0)
### Tag
`1.0.0`
### Date
Dec. 12th, 2016
### Changes
None.
### Additions
* TCP Client & Server for Unix & Mac platform.
* Documented in the wiki.
### Removals
None.
