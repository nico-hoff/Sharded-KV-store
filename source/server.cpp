#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <linux/sockios.h>
#include <time.h>
#include <sys/time.h>
#include <sys/types.h>
#include <iostream>
#include <sys/select.h>

#include <thread>
#include <pthread.h>
#include <mutex>
#include <tuple>

#include "kv_store.h"
#include "message.h"
#include "shared.h"
#include "workload_traces/generate_traces.h"

#include <cxxopts.hpp>
#include <fmt/printf.h>

#include <dirent.h>
#include <filesystem>
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"

std::mutex m;
struct timeval timeout;

// int no_threads, server_port, no_clients ;
int server_port, master_port;
std::string server_address;
std::vector<int> connections; // std::tuple<int, int>> socket_fds;

class ServerThread
{
	int thread_id = 0;
	struct sockaddr_in srv_addr;

public:
	ServerThread() = delete;
	explicit ServerThread(int tid) : thread_id(tid) {}
};

std::atomic<int> threads_ids{0};

class ServerOP
{
	std::shared_ptr<KvStore> local_kv;

public:
	ServerOP()
	{
		local_kv = KvStore::init();
	}

	void local_kv_init_it() { local_kv->init_it(); }

	bool local_kv_put(int key, std::string_view value)
	{
		return local_kv->put(key, value);
	}

	std::string local_kv_get(int key)
	{
		std::optional<std::string_view> strvw = local_kv->get(key);
		if (strvw == std::nullopt)
		{
			return std::string("NOT-FOUND");
		}
		return std::string(strvw->data());
	}

	auto local_kv_get_next_key() -> int
	{
		return local_kv->get_next_key();
	}

	auto get_local_kv()
	{
		return local_kv;
	}

	void reset_kv()
	{
		local_kv = KvStore::init();
	}
};

std::string get_db(rocksdb::DB &rock_db, int key)
{
	std::string value;
	rocksdb::Status rock_s = rock_db.Get(rocksdb::ReadOptions(), std::to_string(key), &value);
	if (!rock_s.ok())
	{
		return "NOT-FOUND";
		// fmt::print("### {}: {}  ###\n", rock_s.ToString().c_str(), key);
	}
	return value;
}

bool put_db(rocksdb::DB &rock_db, int key, std::string value)
{
	rocksdb::Status rock_s = rock_db.Put(rocksdb::WriteOptions(), std::to_string(key), value);
	if (!rock_s.ok())
	{
		fmt::print("\nPUT Errrrrrrrrrrrrrrrrrrror:\n");
		std::cerr << rock_s.ToString() << std::endl;
		return false;
	}
	return true;
}

void send_all(int master_fd, std::shared_ptr<KvStore> temp_local_kv)
{
	fmt::print("\n---redistribution---\n");

	while (auto kv = temp_local_kv->get_next_key())
	{
		auto value = temp_local_kv->get(kv)->data();
		// usleep(5 * 1000);
		if (kv == -1)
		{
			break;
		}

		std::string cmd = "./build/dev/clt -p 0 -d 0 -m 1025 -o PUT -k ";
		cmd += std::to_string(kv);
		cmd += " -v ";
		cmd += value;
		cmd += " & ";
		// fmt::print("%s\n", cmd.c_str());
		if (system(cmd.c_str()) == -1)
			fmt::print("system error!\n");
	}
	fmt::printf("end of redistribution\n");
}

void server_worker(ServerOP *server_op, rocksdb::DB &rock_db)
{
	while (true)
	{
		usleep(5 * 1000);
		if (!connections.empty())
		{
			// fmt::print("Pre Lock {}\n", socket_fds.size());
			m.lock();
			int client_fd = connections.back(); // auto [work_recv_sock, work_send_sock] = socket_fds.back();
			connections.pop_back();
			m.unlock();
			// fmt::print("After Lock - got {}\n", client_fd);

			setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);

			sockets::client_msg message;
			server::server_response::reply server_response;
			bool keep_running = true;
			bool success = true;

			char tmp[1] = "";

			while (true)
			{
				auto op_id = 0;

				// usleep(5);

				if (recv(client_fd, tmp, 1, MSG_PEEK) <= 0) // work_recv_sock
				{
					// fmt::print("Break!\n");
					break;
				}

				recv_clt_message(client_fd, &message);

				std::string value;
				int key = message.ops(0).key();

				switch (message.ops(0).type())
				{
				case sockets::client_msg_OperationType_GET:
					value = get_db(rock_db, key);
					value = server_op->local_kv_get(key);
					fmt::print("GET < {} - {} >\n", key, value.c_str());
					server_response.set_value(value);
					server_response.set_op_id(1);
					server_response.set_success(success);
					send_svr_message(client_fd, server_response);
					// server_response.PrintDebugString();
					break;
				case sockets::client_msg_OperationType_PUT:
					value = message.ops(0).value();
					success = server_op->local_kv_put(key, value);
					success = put_db(rock_db, key, value);
					server_response.set_value(value);
					server_response.set_op_id(0);
					server_response.set_success(success);
					fmt::print("PUT < {} - {} > [{}]\n", key, value.c_str(), success);
					send_svr_message(client_fd, server_response);
					break;
				case sockets::client_msg_OperationType_TXN_START:
					auto temp_local_kv = server_op->get_local_kv();
					server_op->local_kv_init_it();
					std::thread(send_all, client_fd, temp_local_kv).detach();
					server_op->reset_kv();
					break;
				}
			}
			// server_response.PrintDebugString();
			close_socket(client_fd, 0);
		}
	}
}

void listen_for_connections()
{
	while (true)
	{
		accept_connections(server_port, &connections, 0);
		fmt::print("Connections: {} - last {}\n", connections.size(), connections.back());
	}
}

void master_connection()
{
	int sock_fd = connect_to(master_port, server_address, 0, 0);

	sockets::client_msg message;
	auto *operation_data = message.add_ops();
	operation_data->set_type(sockets::client_msg::INIT);
	operation_data->set_port(server_port);
	send_clt_message(sock_fd, message);
	fmt::print("\nRegistert on master with {}\n", server_port);
	fmt::print("------------------------------\n");
	// message.PrintDebugString();
	close_socket(sock_fd, 0);
}

auto main(int argc, char *argv[]) -> int
{
	cxxopts::Options options(argv[0], "Server for the sockets benchmark");
	options.allow_unrecognised_options().add_options()("p,PORT", "port at which the server listens to client or master requests", cxxopts::value<size_t>())("m,MASTER_PORT", "port at which the master server is listening", cxxopts::value<size_t>())("h,help", "Print help");

	auto args = options.parse(argc, argv);

	if (args.count("help"))
	{
		fmt::print("{}\n", options.help());
		return 0;
	}

	if (!args.count("PORT"))
	{
		fmt::print(stderr, "The port at which the server listens to client or master requests\n{}\n", options.help());
		return 0;
	}

	if (!args.count("MASTER_PORT"))
	{
		fmt::print(stderr, "The port at which the master server is listening\n{}\n", options.help());
		return 0;
	}

	server_port = args["PORT"].as<size_t>();
	master_port = args["MASTER_PORT"].as<size_t>();
	server_address = "127.0.0.1";

	// auto id = threads_ids.fetch_add(1);
	// ServerThread m_thread(id);

	rocksdb::DB *rock_db;
	rocksdb::Options rock_options;
	rocksdb::Status rock_s;

	rock_options.IncreaseParallelism(); // no_threads);
	rock_options.OptimizeLevelStyleCompaction();
	rock_options.compression_per_level.resize(rock_options.num_levels);
	rock_options.create_if_missing = true;

	for (int i = 0;
		 i < rock_options.num_levels; i++)
	{
		rock_options.compression_per_level[i] = rocksdb::kNoCompression;
	}

	rock_options.compression = rocksdb::kNoCompression;

	std::string root;
	std::filesystem::path cwd = std::filesystem::current_path();
	std::string main = "/rockDBs/sub_DB_";

	for (int i = 0; ; i++)
	{
		root = cwd.c_str() + main + std::to_string(i);
		rock_s = rocksdb::DB::Open(rock_options, root, &rock_db);
		if (!rock_s.IsIOError())
			break;
	}
	fmt::print("\nRockDB is {} at {}\n", rock_s.ToString().c_str(), root.c_str());
	assert(rock_s.ok());

	std::vector<std::thread> threads;

	ServerOP server_op;
	server_op.local_kv_init_it();

	timeout.tv_sec = 3;	 // 3;
	timeout.tv_usec = 0; // 5 * 1000 * 100;

	master_connection();

	for (int i = 0; i < 1; i++) // 4
		threads.emplace_back(listen_for_connections);

	for (int i = 0; i < 1; i++) // 1
		threads.emplace_back(server_worker, &server_op, std::ref(*rock_db));

	for (auto &thread : threads)
	{
		thread.join();
	}

	fmt::print("** all threads joined **\n");

	return 0;
}