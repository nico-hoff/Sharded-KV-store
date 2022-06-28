#include <algorithm>
#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include <functional>

#include <arpa/inet.h>
#include <cxxopts.hpp>
#include <fcntl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/text_format.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <barrier>

#include "client_thread.h"
#include "kv_store.h"
#include "message.h"
#include "shared.h"
#include "workload_traces/generate_traces.h"

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::atomic<int> threads_ids{0};
int nb_clients = -1;
int nb_messages = 1200;
int port, key, master_port, direct;
std::string server_address = "127.0.0.1";
std::string operation, value;
std::vector<::Workload::TraceCmd> traces;
struct timeval timeout;

class Barriers
{
public:
	explicit Barriers(int nb_client_threads)
	{
		sync_point = std::make_shared<std::barrier<>>(nb_client_threads);
		terminate_point = std::make_shared<std::barrier<>>(nb_client_threads);
		start_point = std::make_shared<std::barrier<>>(nb_client_threads);
	}

	std::shared_ptr<std::barrier<>> sync_point;
	std::shared_ptr<std::barrier<>> terminate_point;
	std::shared_ptr<std::barrier<>> start_point;
};

std::shared_ptr<Barriers> barriers;

class ClientOP
{
	static constexpr auto max_n_operations = 100ULL;
	// NOLINTNEXTLINE (cert-err58-cpp)
	static inline std::string const random_string =
		"lllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllllll"
		"l";
	static std::string rand_str(size_t sz = 64)
	{
		static const char charset[] = "0123456789"
									  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
									  "abcdefghijklmnopqrstuvwxyz";

		std::string s("d");
		s.reserve(sz);
		for (size_t i = 0UL; i < sz; i++)
		{
			s += charset[rand() % (sizeof(charset) - 1)];
		}
		return s;
	}
	std::atomic<size_t> global_number{0ULL};
	std::atomic<size_t> number_of_iterations{0ULL};
	// FIXME I am pretty sure this is not necessary
	std::atomic<size_t> number_of_requests{1ULL};
	std::atomic<uint64_t> no_puts = 0;
	// this is used for verification
	std::shared_ptr<KvStore> local_kv;

	void get_operation_put(sockets::client_msg::OperationData *operation_data,
						   std::vector<::Workload::TraceCmd>::iterator it, ClientThread &client_c)
	{
		auto item = it->operation[0];
		operation_data->set_key(item.key_hash);
		global_number.fetch_add(1, std::memory_order_relaxed);
		operation_data->set_type(sockets::client_msg::PUT);
		auto rstring = rand_str();
		operation_data->set_value(rstring);
		client_c.local_kv_put(item.key_hash, rstring);
		// fmt::print("[{}] key={}, value={}\n", __func__, item.key_hash, rstring);
		no_puts.fetch_add(1);
	}

	void get_operation_get(sockets::client_msg::OperationData *operation_data,
						   std::vector<::Workload::TraceCmd>::iterator it, ClientThread &client_c)
	{
		auto item = it->operation[0];
		operation_data->set_key(item.key_hash);
		global_number.fetch_sub(1, std::memory_order_relaxed);
		operation_data->set_type(sockets::client_msg::GET);
	}

	void
	get_operation_txn_start(sockets::client_msg::OperationData *operation_data,
							std::vector<::Workload::TraceCmd>::iterator it)
	{
		operation_data->set_key(1);
		global_number.fetch_sub(1, std::memory_order_relaxed);
		operation_data->set_type(sockets::client_msg::PUT);
	}

	auto get_number_of_requests() -> size_t
	{
		return 1;
		// FIXME probably overly pessimistic
		// FIXME we could simplify it by doing fetch_add and modulo
		auto res = number_of_requests.load(std::memory_order_relaxed);
		while (!number_of_requests.compare_exchange_weak(
			res, res < max_n_operations ? res + 1 : 0ULL, std::memory_order_acq_rel,
			std::memory_order_relaxed))
		{
		}
		return res;
	}

public:
	ClientOP() { local_kv = KvStore::init(); }

	auto get_key(uint32_t key) -> std::tuple<size_t, std::unique_ptr<char[]>>
	{
		sockets::client_msg msg;

		auto *operation_data = msg.add_ops();
		operation_data->set_key(key);
		operation_data->set_type(sockets::client_msg::GET);

		std::string msg_str;
		msg.SerializeToString(&msg_str);

		auto msg_size = msg_str.size();
		auto buf = std::make_unique<char[]>(msg_size + length_size_field);
		convert_int_to_byte_array(buf.get(), msg_size);
		memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
		return {msg_size + length_size_field, std::move(buf)};
	}

	auto get_tx_type(int op)
	{
		if (op == ::Workload::TraceCmd::txn_start)
		{
			return sockets::client_msg::TXN_START;
		}
		if (op == ::Workload::TraceCmd::txn_put)
		{
			return sockets::client_msg::TXN_PUT;
		}
		if (op == ::Workload::TraceCmd::txn_get)
		{
			return sockets::client_msg::TXN_GET;
		}
		if (op == ::Workload::TraceCmd::txn_get_and_execute)
		{
			return sockets::client_msg::TXN_GET_AND_EXECUTE;
		}
		if (op == ::Workload::TraceCmd::txn_commit)
		{
			return sockets::client_msg::TXN_COMMIT;
		}
		if (op == ::Workload::TraceCmd::txn_rollback)
		{
			return sockets::client_msg::TXN_ABORT;
		}
	}

	auto get_tx(std::vector<::Workload::TraceCmd>::iterator &it, int thread_id)
		-> std::tuple<size_t, std::unique_ptr<char[]>, int>
	{
		// todo::@dimitra
		static int tx_ids = 0;

		sockets::client_msg msg;
		auto op_nb = 0;
		for (auto &op : it->operation)
		{
			auto *operation_data = msg.add_ops();
			operation_data->set_client_id(thread_id);
			operation_data->set_txn_id(tx_ids);
			operation_data->set_op_id(op_nb++);
			operation_data->set_key(op.key_hash);
			operation_data->set_value(op.value);
			operation_data->set_type(get_tx_type(op.op));
		}

		tx_ids++;
		std::string msg_str;
		msg.SerializeToString(&msg_str);

		auto msg_size = msg_str.size();
		if (msg_size == 0)
		{
			fmt::print("[{}] ERROR: it->operation.size()={}\n", __func__,
					   it->operation.size());
			std::cout << msg.DebugString() << "\n";
			sleep(2);
		}
		if (it != (traces.end() - 1))
		{
			it++;
		}
		else
		{
			fmt::print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> [{}] restart iterator\n", __func__);
			it = traces.begin() +
				 (traces.size() / nb_clients) * (rand() % (nb_clients - 1));
		}

		auto buf = std::make_unique<char[]>(msg_size + length_size_field);
		convert_int_to_byte_array(buf.get(), msg_size);
		memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
		return {msg_size + length_size_field, std::move(buf), op_nb};
	}

	auto get_operation(std::vector<::Workload::TraceCmd>::iterator &it, ClientThread &client_c)
		-> std::tuple<size_t, std::unique_ptr<char[]>, int>
	{
		auto operation_func = [it]
		{
			// fmt::print("[{}]: {} {}\n", __func__, it->op, it->key_hash);
			if (it->operation[0].op == Workload::TraceCmd::put)
			{
				return &ClientOP::get_operation_put;
			}
			if (it->operation[0].op == Workload::TraceCmd::get)
			{
				return &ClientOP::get_operation_get;
			}
		}();

		sockets::client_msg msg;

		auto *operation_data = msg.add_ops();
		(this->*operation_func)(operation_data, it, client_c);

		if (it != (traces.end() - 1))
		{
			it++;
		}
		else
		{
			fmt::print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> [{}] restart iterator\n", __func__);
			it = traces.begin() +
				 (traces.size() / nb_clients) * (rand() % (nb_clients - 1));
		}

		std::string msg_str;
		msg.SerializeToString(&msg_str);

		auto msg_size = msg_str.size();
		auto buf = std::make_unique<char[]>(msg_size + length_size_field);
		convert_int_to_byte_array(buf.get(), msg_size);
		memcpy(buf.get() + length_size_field, msg_str.data(), msg_size);
		return {msg_size + length_size_field, std::move(buf), 1};
	}

	void verify_all(ClientThread &c_thread)
	{
		c_thread.local_kv_init_it();
		auto verify_nb = 0;
		while (auto kv = c_thread.local_kv_get_next_key())
		{
			if (kv == -1)
				break;
			// fmt::print("{} - {}\n", kv.first, kv.second);
			verify_nb++;
			auto [size, buf] = get_key(kv);
			c_thread.sent_request(buf.get(), size);
			auto [bytecount, result] = c_thread.recv_ack();

			server::server_response::reply msg;
			auto payload_sz = bytecount;
			std::string tmp(result.get(), payload_sz);
			msg.ParseFromString(tmp);
			auto expected_val = c_thread.local_kv_get(kv);
			verify(kv, msg.value().c_str(), msg.value().size(), expected_val);
			//	fmt::print("{} verify={}\r", __func__, verify_nb);
		}
		fmt::print("[{}] Thread_id={} no_puts={}/verifications={}\n", __func__, c_thread.get_thread_id(), no_puts.load(), verify_nb);
	}

	void verify(int key, const char *ret_val, size_t bytecount, std::optional<std::string_view> expected_val)
	{
		// auto expected_val = local_kv->get(key);
		if (expected_val->data() == nullptr)
		{
			fmt::print("[{}] it is nullptr\n", __func__);
			if (bytecount != 0)
				exit(8);
			return;
		}
		if (bytecount == 0)
		{
			fmt::print("[{}] received nullptr\n", __func__);
			exit(8);
		}
		if (::memcmp(ret_val, expected_val->data(), bytecount) != 0)
		{
			fmt::print("[{}] ERROR on key={} {} != {}\n", __func__, key, ret_val,
					   expected_val->data());
			exit(1);
		}
#if 0
		else {
			fmt::print("[{}] all good ret_val={}\n", __func__, ret_val);
		}
#endif
	}
};

int client(int port, std::string operation, int key, std::string value, int master_port, int direct)
{
	sockets::client_msg operation_msg;
	auto *operation_data = operation_msg.add_ops();

	operation_data->set_key(key);
	if (std::strcmp(operation.c_str(), "GET") == 0)
	{
		operation_data->set_type(sockets::client_msg_OperationType_GET);
	}

	if (std::strcmp(operation.c_str(), "PUT") == 0)
	{
		operation_data->set_type(sockets::client_msg_OperationType_PUT);
		operation_data->set_value(value);
	}

	int master_fd, server_port;
	if (direct == 0)
	{
		master_fd = connect_to(master_port, server_address, 0, 0);
		// printf("Connected on %d\n", master_port);
		send_clt_message(master_fd, operation_msg);
		// usleep(5 * 1000 * 100);
		sockets::client_msg master_msg;
		recv_clt_message(master_fd, &master_msg);
		// master_msg.PrintDebugString();
		close_socket(master_fd, 0);
		server_port = master_msg.ops(0).port();
	}
	else
	{
		server_port = port;
	}

	int server_fd = -1;
	while (server_fd == -1)
		server_fd = connect_to(server_port, server_address, 0, 1);

	// setsockopt(server_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof timeout);

	// fmt::print("Connected on {}\n", server_port);
	send_clt_message(server_fd, operation_msg);
	// fmt::print("Message sent to {}\n", server_port);
	// usleep(5 * 1000);
	server::server_response::reply server_msg;
	recv_svr_message(server_fd, &server_msg);
	close_socket(server_fd, 0);

	// server_msg.PrintDebugString();
	if (std::strcmp(server_msg.value().c_str(), "NOT-FOUND") == 0)
		return 2;
	if (server_msg.success())
		return 0;
	return 1;
}

auto main(int argc, char *argv[]) -> int
{
	cxxopts::Options options(argv[0], "Client for the sockets benchmark");
	options.allow_unrecognised_options().add_options()("p,PORT", "port at which the target server listens to. This parameter should only be valid when DIRECT is set to 1", cxxopts::value<size_t>())("o,OPERATION", "either a GET or PUT request. The testing script will specify the operations in uppercase characters", cxxopts::value<std::string>())("k,KEY", "key for the operation", cxxopts::value<size_t>())("v,VALUE", "value for the operation corresponding to the key. Only valid if the OPERATION is PUT.", cxxopts::value<std::string>())("m,MASTER_PORT", "Port at which the master listens to for the client.", cxxopts::value<size_t>())("d,DIRECT", "Specifies whether the client can talk to the server at port PORT. It is important that the implementation of your client can talk directly to server at PORT. It is set to 0 meaning false, or 1 meaning true i.e. the client talks to the server directly without the help from master.", cxxopts::value<size_t>())("h,help", "Print help");

	auto args = options.parse(argc, argv);
	if (args.count("help"))
	{
		fmt::print("{}\n", options.help());
		return 0;
	}

	if (!args.count("DIRECT"))
	{
		fmt::print(stderr, "The direct flag is required\n{}\n", options.help());
		return 1;
	}
	direct = args["DIRECT"].as<size_t>();

	if ((direct == 1) && !args.count("PORT"))
	{
		fmt::print(stderr, "The port is required\n{}\n",
				   options.help());
		return 1;
	}

	if (!args.count("OPERATION"))
	{
		fmt::print(stderr, "The operation type is required\n{}\n", options.help());
		return 1;
	}

	if (!args.count("KEY"))
	{
		fmt::print(stderr, "The key is required\n{}\n", options.help());
		return 1;
	}

	if (!args.count("VALUE"))
	{ // only if put
		fmt::print(stderr, "The value is required\n{}\n",
				   options.help());
		return 1;
	}

	if (!args.count("MASTER_PORT"))
	{
		fmt::print(stderr, "The master port is required\n{}\n", options.help());
		return 1;
	}

	// NOLINTNEXTLINE(concurrency-mt-unsafe)
	// hostip = gethostbyname("localhost");

	if (direct == 1)
		port = args["PORT"].as<size_t>();
	else
		port = -1;
	operation = args["OPERATION"].as<std::string>();
	key = args["KEY"].as<size_t>();
	value = args["VALUE"].as<std::string>();
	master_port = args["MASTER_PORT"].as<size_t>();

	timeout.tv_sec = 3;
	timeout.tv_usec = 0;

	int client_state = client(port, operation, key, value, master_port, direct);
	printf("Client finshed with %d.\n", client_state);
	return client_state;
}
