/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "server.h"
#include "../util/strings.h"
#include "../util/file.h"
#include "../util/config.h"
#include "../util/log.h"
#include "../util/ip_filter.h"
#include "link.h"
#include <vector>

static DEF_PROC(ping);
static DEF_PROC(info);
static DEF_PROC(auth);
static DEF_PROC(list_allow_ip);
static DEF_PROC(add_allow_ip);
static DEF_PROC(del_allow_ip);
static DEF_PROC(list_deny_ip);
static DEF_PROC(add_deny_ip);
static DEF_PROC(del_deny_ip);

#define TICK_INTERVAL          100 // ms
#define STATUS_REPORT_TICKS    (300 * 1000/TICK_INTERVAL) // second
static const int READER_THREADS = 10;
static const int WRITER_THREADS = 1;  // 必须为1, 因为某些写操作依赖单线程

volatile bool quit = false;
volatile uint32_t g_ticks = 0;

void signal_handler(int sig){
	switch(sig){
		case SIGTERM:
		case SIGINT:{
			quit = true;
			break;
		}
		case SIGALRM:{
			g_ticks ++;
			break;
		}
	}
}

NetworkServer::NetworkServer(){
	num_readers = READER_THREADS;
	num_writers = WRITER_THREADS;
	
	tick_interval = TICK_INTERVAL;
	status_report_ticks = STATUS_REPORT_TICKS;

	//conf = NULL;
	serv_link = NULL;
	link_count = 0;

	fdes = new Fdevents();
	ip_filter = new IpFilter();

	// add built-in procs, can be overridden
	proc_map.set_proc("ping", "r", proc_ping);
	proc_map.set_proc("info", "r", proc_info);
	proc_map.set_proc("auth", "r", proc_auth);
	proc_map.set_proc("list_allow_ip", "r", proc_list_allow_ip);
	proc_map.set_proc("add_allow_ip",  "r", proc_add_allow_ip);
	proc_map.set_proc("del_allow_ip",  "r", proc_del_allow_ip);
	proc_map.set_proc("list_deny_ip",  "r", proc_list_deny_ip);
	proc_map.set_proc("add_deny_ip",   "r", proc_add_deny_ip);
	proc_map.set_proc("del_deny_ip",   "r", proc_del_deny_ip);

	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, signal_handler);
	signal(SIGTERM, signal_handler);
#ifndef __CYGWIN__
	signal(SIGALRM, signal_handler);
	{
		struct itimerval tv;
		tv.it_interval.tv_sec = (TICK_INTERVAL / 1000);
		tv.it_interval.tv_usec = (TICK_INTERVAL % 1000) * 1000;
		tv.it_value.tv_sec = 1;
		tv.it_value.tv_usec = 0;
		setitimer(ITIMER_REAL, &tv, NULL);
	}
#endif
}
	
NetworkServer::~NetworkServer(){
	//delete conf;
	delete serv_link;
	delete fdes;
	delete ip_filter;

	writer->stop();
	delete writer;
	reader->stop();
	delete reader;
}

NetworkServer* NetworkServer::init(const char *conf_file, int num_readers, int num_writers){
	if(!is_file(conf_file)){
		fprintf(stderr, "'%s' is not a file or not exists!\n", conf_file);
		exit(1);
	}

	Config *conf = Config::load(conf_file);
	if(!conf){
		fprintf(stderr, "error loading conf file: '%s'\n", conf_file);
		exit(1);
	}
	{
		std::string conf_dir = real_dirname(conf_file);
		if(chdir(conf_dir.c_str()) == -1){
			fprintf(stderr, "error chdir: %s\n", conf_dir.c_str());
			exit(1);
		}
	}
	NetworkServer* serv = init(*conf, num_readers, num_writers);
	delete conf;
	return serv;
}

NetworkServer* NetworkServer::init(const Config &conf, int num_readers, int num_writers){
	static bool inited = false;
	if(inited){
		return NULL;
	}
	inited = true;
	
	NetworkServer *serv = new NetworkServer();
	if(num_readers >= 0){
		serv->num_readers = num_readers;
	}
	if(num_writers >= 0){
		serv->num_writers = num_writers;
	}
	// init ip_filter
	{
		Config *cc = (Config *)conf.get("server");
		if(cc != NULL){
			std::vector<Config *> *children = &cc->children;
			std::vector<Config *>::iterator it;
			for(it = children->begin(); it != children->end(); it++){
				if((*it)->key == "allow"){
					const char *ip = (*it)->str();
					log_info("    allow %s", ip);
					serv->ip_filter->add_allow(ip);
				}
				if((*it)->key == "deny"){
					const char *ip = (*it)->str();
					log_info("    deny %s", ip);
					serv->ip_filter->add_deny(ip);
				}
			}
		}
	}
	
	{ // server
		const char *ip = conf.get_str("server.ip");
		int port = conf.get_num("server.port");
		if(ip == NULL || ip[0] == '\0'){
			ip = "127.0.0.1";
		}
		
		serv->serv_link = Link::listen(ip, port);
		if(serv->serv_link == NULL){
			log_fatal("error opening server socket! %s", strerror(errno));
			fprintf(stderr, "error opening server socket! %s\n", strerror(errno));
			exit(1);
		}
		log_info("server listen on %s:%d", ip, port);

		std::string password;
		password = conf.get_str("server.auth");
		if(password.size() && (password.size() < 32 || password == "very-strong-password")){
			log_fatal("weak password is not allowed!");
			fprintf(stderr, "WARNING! Weak password is not allowed!\n");
			exit(1);
		}
		if(password.empty()){
			log_info("auth: off");
		}else{
			log_info("auth: on");
		}
		serv->need_auth = false;		
		if(!password.empty()){
			serv->need_auth = true;
			serv->password = password;
		}
	}
	return serv;
}

void NetworkServer::serve(){
	//生成leveldb写操作的线程池
	writer = new ProcWorkerPool("writer");
	writer->start(num_writers);
	//生成leveldb读ß操作的线程池
	reader = new ProcWorkerPool("reader");
	reader->start(num_readers);

	ready_list_t ready_list;
	ready_list_t ready_list_2;
	ready_list_t::iterator it;
	const Fdevents::events_t *events;

	// 开始时设置三个需要监听的文件描述符，serv_link监听新连接到来，reader读任务线程池，write写任务线程池
	fdes->set(serv_link->fd(), FDEVENT_IN, 0, serv_link);
	fdes->set(this->reader->fd(), FDEVENT_IN, 0, this->reader);
	fdes->set(this->writer->fd(), FDEVENT_IN, 0, this->writer);
	
	uint32_t last_ticks = g_ticks;
	
	while(!quit){
		// status report
		if((uint32_t)(g_ticks - last_ticks) >= STATUS_REPORT_TICKS){
			last_ticks = g_ticks;
			log_info("server running, links: %d", this->link_count);
		}
		
		ready_list.swap(ready_list_2);
		ready_list_2.clear();
		
		if(!ready_list.empty()){
			// ready_list not empty, so we should return immediately
			events = fdes->wait(0);
		}else{
			events = fdes->wait(50);
		}
		if(events == NULL){
			log_fatal("events.wait error: %s", strerror(errno));
			break;
		}
		// 处理有读写事件发生的连接，将准备好的连接添加到ready_list中
		for(int i=0; i<(int)events->size(); i++){
			const Fdevent *fde = events->at(i);
			// 1.事件发生在监听Link上，说明是新的连接到达
			if(fde->data.ptr == serv_link){
				Link *link = accept_link();
				if(link){
					this->link_count ++;				
					log_debug("new link from %s:%d, fd: %d, links: %d",
						link->remote_ip, link->remote_port, link->fd(), this->link_count);
					// 将新的连接添加到监听队列中
					fdes->set(link->fd(), FDEVENT_IN, 1, link);
				}
			// 2. 事件发生在两个任务队列上，说明有任务完成，调用proc_result处理结果	
			}else if(fde->data.ptr == this->reader || fde->data.ptr == this->writer){
				ProcWorkerPool *worker = (ProcWorkerPool *)fde->data.ptr;
				ProcJob *job;
				if(worker->pop(&job) == 0){
					log_fatal("reading result from workers error!");
					exit(0);
				}
				if(proc_result(job, &ready_list) == PROC_ERROR){
					//
				}
			// 3. 处理其他客户端连接，这里将对客户端发送来的数据进行处理，解析客户端的请求
			}else{
				proc_client_event(fde, &ready_list);
			}
		}

		for(it = ready_list.begin(); it != ready_list.end(); it ++){
			Link *link = *it;
			if(link->error()){
				this->link_count --;
				fdes->del(link->fd());
				delete link;
				continue;
			}
			// link的recv函数将解析收到的数据
			const Request *req = link->recv();
			if(req == NULL){
				log_warn("fd: %d, link parse error, delete link", link->fd());
				this->link_count --;
				fdes->del(link->fd());
				delete link;
				continue;
			}
			// req为空，说明没有解析出完整的请求，仍然需要监听读事件，期待继续读入数据解析
			if(req->empty()){
				fdes->set(link->fd(), FDEVENT_IN, 1, link);
				continue;
			}
			// 如果收到的数据已经解析完成，则创建JOB对象供任务线程池处理
			//走到此处表明，已经有一个完成的命令读取完毕
			link->active_time = millitime();

			ProcJob *job = new ProcJob();
			job->link = link;
			job->req = link->last_recv();
			int result = this->proc(job);
			//生成一个新的JOB，并抛给后端工作线程处理
			if(result == PROC_THREAD){
				fdes->del(link->fd());
				continue;
			}
			if(result == PROC_BACKEND){
				fdes->del(link->fd());
				this->link_count --;
				continue;
			}
			// 有一些任务在proc函数中由当前线程处理完成，因此添加到ready_list_2中，
			// 在外层while循环的开头，会交换ready_list_2和ready_list，然后处理ready_list
			if(proc_result(job, &ready_list_2) == PROC_ERROR){
				//
			}
		} // end foreach ready link
	}
}

Link* NetworkServer::accept_link(){
	Link *link = serv_link->accept();
	if(link == NULL){
		log_error("accept failed! %s", strerror(errno));
		return NULL;
	}
	if(!ip_filter->check_pass(link->remote_ip)){
		log_debug("ip_filter deny link from %s:%d", link->remote_ip, link->remote_port);
		delete link;
		return NULL;
	}
				
	link->nodelay();
	link->noblock();
	link->create_time = millitime();
	link->active_time = link->create_time;
	return link;
}

// 对任务结果的处理放在proc_result函数中，在这里，会将结果发送给客户端
int NetworkServer::proc_result(ProcJob *job, ready_list_t *ready_list){
	Link *link = job->link;
	int result = job->result;
			
	if(log_level() >= Logger::LEVEL_DEBUG){
		log_debug("w:%.3f,p:%.3f, req: %s, resp: %s",
			job->time_wait, job->time_proc,
			serialize_req(*job->req).c_str(),
			serialize_req(job->resp.resp).c_str());
	}
	// 用于统计计数
	if(job->cmd){
		job->cmd->calls += 1;
		job->cmd->time_wait += job->time_wait;
		job->cmd->time_proc += job->time_proc;
	}
	delete job;
	
	if(result == PROC_ERROR){
		log_info("fd: %d, proc error, delete link", link->fd());
		goto proc_err;
	}
	
	if(!link->output->empty()){
		// 这里将任务产生的结果发送给客户端
		int len = link->write();
		//log_debug("write: %d", len);
		if(len < 0){
			log_debug("fd: %d, write: %d, delete link", link->fd(), len);
			goto proc_err;
		}
	}
	// 如果输出缓冲区不为空，则说明还有数据未完全发送给客户端，需要关注写事件
	if(!link->output->empty()){
		fdes->set(link->fd(), FDEVENT_OUT, 1, link);
	}
	// 如果输入缓冲区为空，说明客户端发送过来的数据已经全部解析完成，继续关注读事件
	if(link->input->empty()){
		fdes->set(link->fd(), FDEVENT_IN, 1, link);
	}else{
		// 读缓冲区不为空, 说明还有输入的数据需要处理，将link放到ready_list里面等待处理，同时
		// 暂时不再关注该连接上的读事件
		fdes->clr(link->fd(), FDEVENT_IN);
		ready_list->push_back(link);
	}
	return PROC_OK;

proc_err:
	this->link_count --;
	fdes->del(link->fd());
	delete link;
	return PROC_ERROR;
}

/*
event:
	read => ready_list OR close
	write => NONE
proc =>
	done: write & (read OR ready_list)
	async: stop (read & write)
	
1. When writing to a link, it may happen to be in the ready_list,
so we cannot close that link in write process, we could only
just mark it as closed.

2. When reading from a link, it is never in the ready_list, so it
is safe to close it in read process, also safe to put it into
ready_list.

3. Ignore FDEVENT_ERR

A link is in either one of these places:
	1. ready list
	2. async worker queue
So it safe to delete link when processing ready list and async worker result.
*/
/*
proc_client_event处理客户端的交互:
	1.处理读事件，将数据从内核缓冲区读到link自带的应用层缓冲区(后面将对应用层缓冲区中的数据进行解析，获取客户端的真实请求)。
	2.处理写事件，将结果数据发送给客户端(将Link的写缓冲区中的数据交给内核发送)。
*/
int NetworkServer::proc_client_event(const Fdevent *fde, ready_list_t *ready_list){
	Link *link = (Link *)fde->data.ptr;
	// 连接上有数据可读，则读到link的缓冲区中，并将link添加到ready_list等待接下来的处理
	if(fde->events & FDEVENT_IN){
		ready_list->push_back(link);
		if(link->error()){
			return 0;
		}
		// 读取数据到接收缓冲区
		int len = link->read();
		//log_debug("fd: %d read: %d", link->fd(), len);
		if(len <= 0){
			log_debug("fd: %d, read: %d, delete link", link->fd(), len);
			link->mark_error();
			return 0;
		}
	}
	// 连接上有写事件发生，将输出缓冲区中的数据写到客户端，这里不会将link添加到ready_list，因为向客户端写数据说明该连接上的请求已处理完成，解析来只用将数据全部写到客户端即可
	if(fde->events & FDEVENT_OUT){
		if(link->error()){
			return 0;
		}
		// 写数据
		int len = link->write();
		if(len <= 0){
			log_debug("fd: %d, write: %d, delete link", link->fd(), len);
			link->mark_error();
			return 0;
		}
		// 如果全部数据写完了，则从监听列表中清除该连接的写事件
		if(link->output->empty()){
			fdes->clr(link->fd(), FDEVENT_OUT);
		}
	}
	return 0;
}

/*
对ready_list的处理：
	1.对ready_list中的每个Link调用recv函数解析，recv函数会根据格式试图从Link的读缓冲区中解析出完整的请求，如果当前的数据还不够解析出一个完整的请求，则返回的req为empty，
	  这样会继续监听该Link的读事件，希望读取更多的数据。如果解析出完整的请求，将构造JOB(任务)分发处理。
	2.proc函数将根据JOB对任务进行分发处理：读任务线程池，写任务线程池，当前线程处理。
*/
int NetworkServer::proc(ProcJob *job){
	job->serv = this;
	job->result = PROC_OK;
	job->stime = millitime();

	const Request *req = job->req;

	do{
		// AUTH
		// 检查是否需要Auth，否则发送失败的Response
		if(this->need_auth && job->link->auth == false && req->at(0) != "auth"){
			job->resp.push_back("noauth");
			job->resp.push_back("authentication required");
			break;
		}
		// 从注册的处理函数表中找出对应的处理函数构造Command
		Command *cmd = proc_map.get_proc(req->at(0));
		if(!cmd){
			job->resp.push_back("client_error");
			job->resp.push_back("Unknown Command: " + req->at(0).String());
			break;
		}
		job->cmd = cmd;
		// FLAG_THREAD:表明任务需要在其他线程中执行
		if(cmd->flags & Command::FLAG_THREAD){
			// 表明是需要在子线程中执行的写任务
			if(cmd->flags & Command::FLAG_WRITE){
				writer->push(job);
			}else{
				reader->push(job);
			}
			return PROC_THREAD;
		}
		// 某些任务不用再子线程中执行，直接在主线程中执行并构造处理结果
		proc_t p = cmd->proc;
		job->time_wait = 1000 * (millitime() - job->stime);
		job->result = (*p)(this, job->link, *req, &job->resp);
		job->time_proc = 1000 * (millitime() - job->stime) - job->time_wait;
	}while(0);
	// send函数只是将Response放到link的写缓冲区中，并不真正的发送给客户端
	if(job->link->send(job->resp.resp) == -1){
		job->result = PROC_ERROR;
	}
	return job->result;
}


/* built-in procs */

static int proc_ping(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	return 0;
}

static int proc_info(NetworkServer *net, Link *link, const Request &req, Response *resp){
	resp->push_back("ok");
	resp->push_back("ideawu's network server framework");
	resp->push_back("version");
	resp->push_back("1.0");
	resp->push_back("links");
	resp->add(net->link_count);
	{
		int64_t calls = 0;
		proc_map_t::iterator it;
		for(it=net->proc_map.begin(); it!=net->proc_map.end(); it++){
			Command *cmd = it->second;
			calls += cmd->calls;
		}
		resp->push_back("total_calls");
		resp->add(calls);
	}
	return 0;
}

static int proc_auth(NetworkServer *net, Link *link, const Request &req, Response *resp){
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		if(!net->need_auth || req[1] == net->password){
			link->auth = true;
			resp->push_back("ok");
			resp->push_back("1");
		}else{
			resp->push_back("error");
			resp->push_back("invalid password");
		}
	}
	return 0;
}

#define ENSURE_LOCALHOST() do{ \
		if(strcmp(link->remote_ip, "127.0.0.1") != 0){ \
			resp->push_back("noauth"); \
			resp->push_back("this command is only available from 127.0.0.1"); \
			return 0; \
		} \
	}while(0)

static int proc_list_allow_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();

	resp->push_back("ok");
	IpFilter *ip_filter = net->ip_filter;
	if(ip_filter->allow_all){
		resp->push_back("all");
	}
	std::set<std::string>::const_iterator it;
	for(it=ip_filter->allow.begin(); it!=ip_filter->allow.end(); it++){
		std::string ip = *it;
		ip = ip.substr(0, ip.size() - 1);
		resp->push_back(ip);
	}

	return 0;
}

static int proc_add_allow_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->add_allow(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

static int proc_del_allow_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->del_allow(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

static int proc_list_deny_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();

	resp->push_back("ok");
	IpFilter *ip_filter = net->ip_filter;
	if(!ip_filter->allow_all){
		resp->push_back("all");
	}
	std::set<std::string>::const_iterator it;
	for(it=ip_filter->deny.begin(); it!=ip_filter->deny.end(); it++){
		std::string ip = *it;
		ip = ip.substr(0, ip.size() - 1);
		resp->push_back(ip);
	}

	return 0;
}

static int proc_add_deny_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->add_deny(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

static int proc_del_deny_ip(NetworkServer *net, Link *link, const Request &req, Response *resp){
	ENSURE_LOCALHOST();
	if(req.size() != 2){
		resp->push_back("client_error");
	}else{
		IpFilter *ip_filter = net->ip_filter;
		ip_filter->del_deny(req[1].String());
		resp->push_back("ok");
	}
	return 0;
}

