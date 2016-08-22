/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#ifndef SSDB_SLOTS_H_
#define SSDB_SLOTS_H_

#include "include.h"
#include <string>
#include <vector>
#include "util/strings.h"
#include "util/thread.h"
#include "util/bytes.h"
#include <string>
#include <vector>

namespace ssdb{
	class Client;
};

inline static
std::string slot_guard(const char type, uint16_t id, const char *key){
	std::string guard;
	guard.append(1, type);
	uint16_t slot = big_endian(id);
	guard.append((char *)&slot, sizeof(uint16_t));
	guard.append(key, 1);
	return guard;
}

class SlotStatus{
public:
	static const char NORMAL		= 0;
	static const char MIGRATING		= 1;
};

class SlotKeyRange{
public:
	std::string kv_begin;
	std::string kv_end;
	std::string hash_begin;
	std::string hash_end;
	std::string queue_begin;
	std::string queue_end;
	std::string zset_begin;
	std::string zset_end;
	SlotKeyRange(){
	}

	SlotKeyRange(const SlotKeyRange &krange){
		this->kv_begin = krange.kv_begin;
		this->kv_end = krange.kv_end;
		this->hash_begin = krange.hash_begin;
		this->hash_end = krange.hash_end;
		this->queue_begin = krange.queue_begin;
		this->queue_end = krange.queue_end;
		this->zset_begin = krange.zset_begin;
		this->zset_end = krange.zset_end;
	}
	bool kv_empty() const{
		return kv_begin == "" && kv_end == "";
	}
	bool hash_empty() const{
		return hash_begin == "" && hash_end == "";
	}
	bool queue_empty() const{
		return queue_begin == "" && queue_end == "";
	}
	bool zset_empty() const{
		return zset_begin == "" && zset_end == "";
	}
	bool empty() const{
		return kv_begin == "" && kv_end == "" && hash_begin == "" && hash_end == "" && queue_begin == "" && queue_end == "" && zset_begin == "" && zset_end == "";
	}
	std::string str() const;
};

class Slot{
public:
	int id;
	SlotKeyRange range;
	Slot(){}
	Slot(const Slot &slot){
		this->id=slot.id;
		this->range=slot.range;
	}
};

class SSDB;

class SlotsManager
{
public:
	SlotsManager(SSDB *db, SSDB *meta);
	~SlotsManager();

	int init_slots();
	std::string slotsinfo();

	int get_slot(int id, Slot *slot);
	Slot* get_slot_ref(int id);
	int get_slot_list(std::vector<Slot> *ids_list);
	int get_slot_ids(std::vector<int> *list);

	int add_slot(int id, SlotKeyRange range);
	int del_slot(int id);

	//int update_slot_key_range(int id, const char type, std::string & key);

	int migrate_slot(std::string addr, int port, int timeout, int slot);
	int migrate_slot_keys(std::string addr, int port, int timeout, int slot);

private:
	SSDB *db;
	SSDB *meta;
	SlotKeyRange load_slot_range(int id);
	std::string get_range_begin(int id, const char tyep);
	std::string get_range_end(int id, const char tyep);

	std::vector<Slot> slots_list;
	std::string slots_hash_key;
	Mutex mutex;
};


#endif
