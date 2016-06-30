/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_BINLOG_H_
#define SSDB_BINLOG_H_

#include <string>
#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/slice.h"
#include "leveldb/status.h"
#include "leveldb/write_batch.h"
#include "../util/thread.h"
#include "../util/bytes.h"


class Binlog{
private:
	// 存储数据的buf,Binlog里面只保存key，没有value，数据格式为: head + key
	std::string buf;
	// Binlog数据由头部和body组成。头部包含uint64_t类型的seq和一个字符的数据同步类型以及一个字节的cmd号。即head:(uint64_t)seq + (char)type + (char)cmd
	static const unsigned int HEADER_LEN = sizeof(uint64_t) + 2;
public:
	Binlog(){}
	Binlog(uint64_t seq, char type, char cmd, const leveldb::Slice &key);
		
	int load(const Bytes &s);	// 从Bytes加载数据
	int load(const leveldb::Slice &s);	// 从Slice加载数据
	int load(const std::string &s);	// 从string加载数据

	uint64_t seq() const;	// 从Binlog字节流里面取seq
	char type() const;		// 从Binlog字节流里面取type
	char cmd() const;		// 从Binlog字节流里面取cmd
	const Bytes key() const;	// 从Binlog字节流里面取key

	const char* data() const{
		return buf.data();
	}
	int size() const{
		return (int)buf.size();
	}
	const std::string repr() const{	// 取内容
		return this->buf;
	}
	std::string dumps() const;	// 格式化成可显示的字符串
};

// circular queue
class BinlogQueue{
private:
	leveldb::DB *db;	// 这个就是要操作的存储数据的db
	uint64_t min_seq;	// 队列中的最小seq，用于同步数据时标识起点。注意该seq不是db中的最小seq。
	uint64_t last_seq;	// db中最新seq，标识当前数据的最大seq
	uint64_t tran_seq;
	int capacity;
	leveldb::WriteBatch batch;	// leveldb的批量写

	volatile bool thread_quit;
	static void* log_clean_thread_func(void *arg);	// 定量清理log的线程函数，在独立线程维护清理工作
	int del(uint64_t seq);	// 按seq删除数据
	// [start, end] includesive
	int del_range(uint64_t start, uint64_t end);	// 按seq的范围删除数据
	
	void clean_obsolete_binlogs();
	void merge();
	bool enabled;
public:
	Mutex mutex;

	BinlogQueue(leveldb::DB *db, bool enabled=true, int capacity=20000000);
	~BinlogQueue();
	void begin();
	void rollback();
	leveldb::Status commit();	// 将batch里面的数据写到db
	// leveldb put
	void Put(const leveldb::Slice& key, const leveldb::Slice& value);	// 写数据提交到batch
	// leveldb delete
	void Delete(const leveldb::Slice& key);	// 删数据提交到batch
	void add_log(char type, char cmd, const leveldb::Slice &key);	// 添加一行Binlog日志到batch
	void add_log(char type, char cmd, const std::string &key);	// 添加一行Binlog日志到batch
		
	int get(uint64_t seq, Binlog *log) const;	// 从db获取一条Binlog日志
	int update(uint64_t seq, char type, char cmd, const std::string &key);	// 直接操作db，写一条Binlog日志到db
		
	void flush();	// 清除db中当前管理的Binlog日志
		
	/** @returns
	 1 : log.seq greater than or equal to seq
	 0 : not found
	 -1: error
	 */
	int find_next(uint64_t seq, Binlog *log) const;	// 根据seq查找下一条Binlog日志
	int find_last(Binlog *log) const;	// 查找最新的Binlog日志
		
	std::string stats() const;	// 合并Binlog日志
};

class Transaction{
private:
	BinlogQueue *logs;
public:
	Transaction(BinlogQueue *logs){
		this->logs = logs;
		logs->mutex.lock();
		logs->begin();
	}
	
	~Transaction(){
		// it is safe to call rollback after commit
		logs->rollback();
		logs->mutex.unlock();
	}
};


#endif
