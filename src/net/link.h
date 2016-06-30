/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef NET_LINK_H_
#define NET_LINK_H_

#include <vector>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>

#include "../util/bytes.h"

#include "link_redis.h"

class Link{
	private:
		// socket fd
		int sock; 
		// socket是否阻塞
		bool noblock_;
		//是否出错
		bool error_;
		//接收数据
		std::vector<Bytes> recv_data;
		// redis 连接
		RedisLink *redis;

		static int min_recv_buf;
		static int min_send_buf;
	public:
		const static int MAX_PACKET_SIZE = 128 * 1024 * 1024;
		// 远端ip
		char remote_ip[INET_ADDRSTRLEN];
		// 远端端口
		int remote_port;
		//是否验证身份
		bool auth;
		bool ignore_key_range;
		// 输入缓冲区
		Buffer *input;
		// 输出缓冲区
		Buffer *output;
		// 创建时间 秒
		double create_time;
		//活跃时间 秒
		double active_time;

		Link(bool is_server=false);
		~Link();
		void close();
		void nodelay(bool enable=true);
		// noblock(true) is supposed to corperate with IO Multiplex,
		// otherwise, flush() may cause a lot unneccessary write calls.
		void noblock(bool enable=true);
		void keepalive(bool enable=true);

		int fd() const{
			return sock;
		}
		bool error() const{
			return error_;
		}
		void mark_error(){
			error_ = true;
		}

		static Link* connect(const char *ip, int port);
		static Link* listen(const char *ip, int port);
		Link* accept();

		// read network data info buffer
		int read();
		int write();
		// flush buffered data to network
		// REQUIRES: nonblock
		int flush();

		/**
		 * parse received data, and return -
		 * NULL: error
		 * empty vector: recv not ready
		 * vector<Bytes>: recv ready
		 */
		const std::vector<Bytes>* recv();
		// wait until a response received.
		const std::vector<Bytes>* response();

		// need to call flush to ensure all data has flush into network
		int send(const std::vector<std::string> &packet);
		int send(const std::vector<Bytes> &packet);
		int send(const Bytes &s1);
		int send(const Bytes &s1, const Bytes &s2);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4);
		int send(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5);

		const std::vector<Bytes>* last_recv(){
			return &recv_data;
		}
		
		/** these methods will send a request to the server, and wait until a response received.
		 * @return
		 * NULL: error
		 * vector<Bytes>: response ready
		 */
		const std::vector<Bytes>* request(const Bytes &s1);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4);
		const std::vector<Bytes>* request(const Bytes &s1, const Bytes &s2, const Bytes &s3, const Bytes &s4, const Bytes &s5);
};

#endif
