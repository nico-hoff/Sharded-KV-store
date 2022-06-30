#include <cstdint>
#include <memory>
#include <optional>
#include <utility>

#include "shared.h"
#include <message.h>

#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <fmt/printf.h>
#include <iostream>

inline auto destruct_message(char *msg, size_t bytes)
	-> std::optional<uint32_t>
{
	if (bytes < 4)
	{
		return std::nullopt;
	}

	auto actual_msg_size = convert_byte_array_to_int(msg);
	return actual_msg_size;
}

static auto read_n(int fd, char *buffer, size_t n) -> size_t
{
	size_t bytes_read = 0;
	size_t retries = 0;
	constexpr size_t max_retries = 10000;
	while (bytes_read < n)
	{
		auto bytes_left = n - bytes_read;
		auto bytes_read_now = recv(fd, buffer + bytes_read, bytes_left, 0);
		// negative return_val means that there are no more data (fine for non
		// blocking socket)
		if (bytes_read_now == 0)
		{
			if (retries >= max_retries)
			{
				return bytes_read;
			}
			++retries;
			continue;
		}
		if (bytes_read_now > 0)
		{
			bytes_read += bytes_read_now;
			retries = 0;
		}
	}
	return bytes_read;
}

auto secure_recv(int fd) -> std::pair<size_t, std::unique_ptr<char[]>>
{
	char dlen[4];

	if (auto byte_read = read_n(fd, dlen, length_size_field);
		byte_read != length_size_field)
	{
		debug_print("[{}] Length of size field does not match got {} expected {}\n",
					__func__, byte_read, length_size_field);
		return {0, nullptr};
	}

	auto actual_msg_size_opt = destruct_message(dlen, length_size_field);
	if (!actual_msg_size_opt)
	{
		debug_print("[{}] Could not get a size from message\n", __func__);
		return {0, nullptr};
	}

	auto actual_msg_size = *actual_msg_size_opt;
	auto buf = std::make_unique<char[]>(static_cast<size_t>(actual_msg_size) + 1);
	buf[actual_msg_size] = '\0';
	if (auto byte_read = read_n(fd, buf.get(), actual_msg_size);
		byte_read != actual_msg_size)
	{
		debug_print("[{}] Length of message is incorrect got {} expected {}\n",
					__func__, byte_read, actual_msg_size);
		return {0, nullptr};
	}

	if (actual_msg_size == 0)
	{
		debug_print("[{}] wrong .. {} bytes\n", __func__, actual_msg_size);
	}
	return {actual_msg_size, std::move(buf)};
}

auto secure_send(int fd, char *data, size_t len) -> std::optional<size_t>
{
	auto bytes = 0LL;
	auto remaining_bytes = len;

	char *tmp = data;

	while (remaining_bytes > 0)
	{
		bytes = send(fd, tmp, remaining_bytes, 0);
		if (bytes < 0)
		{
			// @dimitra: the socket is in non-blocking mode; select() should be also
			// applied
			//             return -1;
			//
			return std::nullopt;
		}
		remaining_bytes -= bytes;
		tmp += bytes;
	}

	return len;
}

int connect_to(int port, std::string server_address, int flag, int timeout_flag)
{
	// init sock_fd -------------------------------------
	int sock_fd;
	if ((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
	{
		fmt::print("socket\n");
		exit(1);
	}

	if (flag == 1)
		fmt::print("Try to connect to {} on {} ..\n", server_address, port);

	sockaddr_in master_addr{};
	master_addr.sin_family = AF_INET;
	master_addr.sin_port = htons(port);
	master_addr.sin_addr.s_addr = inet_addr(server_address.c_str()); // .s_addr = ntohl(INADDR_ANY);  // = *(reinterpret_cast<in_addr *>(he->h_addr));
	memset(&(master_addr.sin_zero), 0, sizeof(master_addr.sin_zero));

	if (timeout_flag != 0 )
	{
		struct timeval timeout = {timeout_flag, 0};
		setsockopt(sock_fd , SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);
	}

	// usleep(5 * 1000 * 100);
	if ((connect(sock_fd, (struct sockaddr *)&master_addr, sizeof(master_addr))) < 0)
	{
		std::cout << "connect Errno: " << errno << std::endl;
		exit(1); // return -1;
	}

	if (flag == 1)
		fmt::print("connect succeeded on sockfd {} ..\n", sock_fd);

	return sock_fd;
}

void accept_connections(int port, std::vector<int> *connections, int flag)
{
	int listen_fd;
	// init recv_sockfd -------------------------------------
	// Create Socket
	if ((listen_fd = socket(AF_INET, SOCK_STREAM, 0)) == -1)
	{
		fmt::print("socket\n");
		exit(1);
	}

	// Make address reusable, if used shortly before
	int opt = 1;
	if ((setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt))) == -1)
	{
		printf("address setsockopt\n");
		std::cout << "Errno: " << errno << std::endl;
		exit(1);
	}

	if ((setsockopt(listen_fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt))) == -1)
	{
		printf("port setsockopt\n");
		std::cout << "Errno: " << errno << std::endl;
		exit(1);
	}

	struct timeval timeout;
	timeout.tv_sec = 5;
	timeout.tv_usec = 0;
	// setsockopt(listen_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);

	// Setting server byte order; declare (local)host IP address;
	// set and convert port number (into network byte order)
	struct sockaddr_in srv_addr; // getaddrinfo
	srv_addr.sin_family = AF_INET;
	srv_addr.sin_port = htons(port);	   // server_port);
	srv_addr.sin_addr.s_addr = INADDR_ANY; // inet_addr(server_address.c_str());
	memset(&(srv_addr.sin_zero), 0, sizeof(srv_addr.sin_zero));

	// passing file descriptor, address structure and the lenght
	// of the address structure to bind current IP on the port
	if ((bind(listen_fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr))) == -1)
	{
		fmt::print("bind\n");
		std::cout << "Errno: " << errno << std::endl;
		exit(1);
	}

	// lets the socket listen for upto 5 connections
	if ((listen(listen_fd, 5)) == -1)
	{
		fmt::printf("listen\n");
		exit(1);
	}

	if (flag == 1)
		fmt::print("{} waiting for new connections on {} ..\n", listen_fd, port);

	int addrlen = sizeof(srv_addr);
	int connected_fd;
	bool still_listening = true;

	while (still_listening)
	{
		if ((connected_fd =
				 accept(listen_fd,
						(struct sockaddr *)&srv_addr,
						(socklen_t *)&addrlen)) < 0)
		{
			fmt::print("accept ");
			std::cout << "Errno: " << errno << std::endl;
			still_listening = false;
			// return -1;
		}

		if (flag == 1)
			fmt::print("accept succeeded on sockfd {} {} ..\n", connected_fd, listen_fd);

		connections->push_back(connected_fd);
	}

	close_socket(listen_fd, flag);

	// return connected_fd;
}

bool recv_clt_message(int sockfd, sockets::client_msg *message)
{
	auto [bytecount, result] = secure_recv(sockfd);

	if (static_cast<int>(bytecount) <= 0 || result == nullptr)
	{
		return false;
	}

	sockets::client_msg temp_msg;
	auto payload_sz = bytecount;
	std::string buffer(result.get(), payload_sz);

	if (!temp_msg.ParseFromString(buffer))
	{
		fmt::print("ParseFromString\n");
		exit(1);
	}

	if (!temp_msg.IsInitialized())
	{
		fmt::print("IsInitialized\n");
		exit(1);
	}

	*message = temp_msg;
	return true;
}

bool recv_svr_message(int sockfd, server::server_response::reply *message)
{
	auto [bytecount, result] = secure_recv(sockfd);

	if (static_cast<int>(bytecount) <= 0 || result == nullptr)
	{
		return false;
	}

	server::server_response::reply temp_msg;
	auto payload_sz = bytecount;
	std::string buffer(result.get(), payload_sz);

	if (!temp_msg.ParseFromString(buffer))
	{
		fmt::print("ParseFromString\n");
		exit(1);
	}

	if (!temp_msg.IsInitialized())
	{
		fmt::print("IsInitialized\n");
		exit(1);
	}

	*message = temp_msg;
	return true;
}

void send_clt_message(int sockfd, sockets::client_msg message)
{
	std::string msg_str;
	message.SerializeToString(&msg_str);

	auto msg_size_payload = msg_str.size();
	auto buf = std::make_unique<char[]>(msg_size_payload + length_size_field);
	construct_message(buf.get(), msg_str.c_str(), msg_size_payload);
	// convert_int_to_byte_array(buf.get(), msg_size);
	// memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);

	secure_send(sockfd, buf.get(), msg_size_payload + length_size_field);
}

void send_svr_message(int sockfd, server::server_response::reply message)
{
	std::string msg_str;
	message.SerializeToString(&msg_str);

	auto msg_size_payload = msg_str.size();
	auto buf = std::make_unique<char[]>(msg_size_payload + length_size_field);
	construct_message(buf.get(), msg_str.c_str(), msg_size_payload);
	// convert_int_to_byte_array(buf.get(), msg_size);
	// memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);

	secure_send(sockfd, buf.get(), msg_size_payload + length_size_field);
}

void close_socket(int sockfd, int flag)
{
	if (flag == 1)
		printf("Closing %d\n", sockfd);
	close(sockfd);
	// sleep(1);
}

void close_sockets(int recv_sockfd, int send_sockfd, int flag)
{
	if (flag == 1)
		printf("Closing %d and %d\n", recv_sockfd, send_sockfd);
	close(recv_sockfd);
	close(send_sockfd);
	// sleep(1);
}