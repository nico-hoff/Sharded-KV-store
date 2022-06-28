#include <cxxopts.hpp>
#include <fmt/printf.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <thread>
#include <vector>
#include <mutex>

#include "kv_store.h"
#include "message.h"
#include "shared.h"

void handle_client(int sockfd, sockets::client_msg message);
void manage_server();
std::map<int, int> map_fds;
int port, master_port, server_fd, num_servers = 0, counter = 0;
std::string server_address;
std::shared_ptr<KvStore> local_kv;
struct sockaddr_in svr_addr;
bool started = false;
std::vector<int> connections;
std::mutex m;
bool manage_block = false;

struct timeval timeout;

class MasterServer
{
	std::shared_ptr<KvStore> local_kv;

public:
	MasterServer()
	{
		local_kv = KvStore::init();
		local_kv->init_it();
	}
};

void redistribute()
{
	fmt::print("blocking manage\n");
	manage_block = true;
	int server_port, server_fd;
	local_kv = KvStore::init();
	for (auto it = map_fds.begin(); it != std::prev(map_fds.end()); it++)
	{
		sockets::client_msg msg;
		auto *operation_data = msg.add_ops();
		operation_data->set_type(sockets::client_msg_OperationType_TXN_START);
		server_port = it->second;
		server_fd = connect_to(server_port, server_address, 0, 0);
		connections.push_back(server_fd);
		send_clt_message(server_fd, msg);
		fmt::print("  notified and pushed {} at {}\n", server_port, server_fd);
		// sleep(1);
	}
	fmt::print("unblocked manage\n");
	// usleep(5 * 1000);
	manage_block = false;
}

void handle_connection(int connected_fd)
{
	sockets::client_msg msg;
	char tmp[1] = "";

	while (true)
	{
		setsockopt(connected_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);

		if (recv(connected_fd, tmp, 1, MSG_PEEK) <= 0)
		{
			// fmt::print("Break!\n");
			break;
		}

		recv_clt_message(connected_fd, &msg);

		if (msg.ops(0).type() == sockets::client_msg_OperationType_INIT)
		{
			map_fds.insert({1 + num_servers++, msg.ops(0).port()});

			if (started)
			{
				fmt::print("\n---redistribution---\n");
				redistribute();
				started = false;
			}
		}
		else if (msg.ops(0).has_type())
		{
			// msg.PrintDebugString();
			// fmt::print("\nClient detected:\n");
			started = true;
			handle_client(connected_fd, msg);
		}
		else
		{
			printf("No type..\n");
			break;
		}
	}
	close_socket(connected_fd, 0);
}

void handle_client(int sockfd, sockets::client_msg message)
{
	int key = message.ops(0).key();
	std::string value = message.ops(0).value();
	local_kv->put(key, value); // mirror

	int shard = (key % num_servers) + 1;
	auto in = map_fds.find(shard);
	int server_port = in->second;

	fmt::print("Forwarding {} to {} with port {}..\n", key, shard, server_port);

	sockets::client_msg client_msg;
	auto *operation_data = client_msg.add_ops();
	operation_data->set_type(sockets::client_msg::INIT);
	operation_data->set_port(server_port);
	// client_msg.PrintDebugString();
	send_clt_message(sockfd, client_msg);
}

void manage_server()
{
	while (true)
	{
		if (!connections.empty() && !manage_block)
		{
			m.lock();
			int connected_fd = connections.back();
			connections.pop_back();
			m.unlock();

			handle_connection(connected_fd);
		}

		if (num_servers != counter)
		{
			fmt::print("\n------------------------------\n");
			fmt::print("Current cluster of {} servers:\n", num_servers);
			for (auto it = map_fds.begin(); it != map_fds.end(); ++it)
				fmt::print("  < {} - {} >\n", it->first, it->second);
			fmt::print("------------------------------\n");
			counter = num_servers;
		}

		usleep(5 * 1000);
	}
}

void listen_for_connections()
{
	int sock_fd;
	while (true)
	{
		sock_fd = accept_connection(master_port, &connections, 0);
		// connections.push_back(sock_fd);
	}
}

void check_health()
{
	while (true)
	{
		sleep(10);
		int server_port;
		for (auto it = map_fds.begin(); it != map_fds.end(); it++)
		{
			server_port = it->second;
			server_fd = connect_to(server_port, server_address, 0, 3);
			if (server_fd > 0)
			{
				fmt::print("--- Server on {} OK\n", server_port);
			}
			else
			{
				fmt::print("--- Server on {} NOT reachable\n", server_port);
				map_fds.erase(it->first);
				num_servers--;
			}
		}
	}
}

// https://www.youtube.com/watch?v=WnlX7w4lHvM Kafka Zookeeper action
// export PS1='\u$ '
// export PS1='\[\033[0;35m\]\u\[\033[0;33m\]$ '

// ./build/dev/master-svr -p 1025
// sudo ./build/dev/svr -m 1025 -p 1026
// ./build/dev/clt -p 0 -d 0 -m 1025 -o PUT -k 1 -v 1000
// sh ./client_runner.sh
// sudo netstat -tunlp

int main(int argc, char const *argv[])
{
	cxxopts::Options options(argv[0], "Master server");
	options.allow_unrecognised_options().add_options()("p,MASTER_PORT", "port at which the master listens to for client requests and new servers joining the cluster", cxxopts::value<size_t>())("h,help", "Print help");

	auto args = options.parse(argc, argv);

	if (args.count("help"))
	{
		fmt::print("{}\n", options.help());
		return 0;
	}

	if (!args.count("MASTER_PORT"))
	{
		fmt::print(stderr, "The port at which the master listens to for client requests and new servers joining the cluster\n{}\n", options.help());
		return 0;
	}

	master_port = args["MASTER_PORT"].as<size_t>();
	server_address = "127.0.0.1";

	std::vector<std::thread> threads;
	// MasterServer *masterOP;

	local_kv = KvStore::init();
	local_kv->init_it();

	timeout.tv_sec = 3;
	timeout.tv_usec = 0;

	fmt::print("listening for connections..\n");
	for (int i = 0; i < 1; i++) // 4
		threads.emplace_back(listen_for_connections);

	threads.emplace_back(manage_server);

	// threads.emplace_back(check_health);

	for (auto &thread : threads)
	{
		thread.join();
	}

	fmt::print("** all threads joined **\n");

	return 0;
}
