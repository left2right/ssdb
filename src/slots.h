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
#include "ssdb/const.h"
#include "ssdb/ssdb.h"
#include "ssdb/ttl.h"
#include "net/link.h"
#include <string>
#include <vector>
//#include <pthread.h>

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
	static const int NORMAL			= 1;
	static const int MIGRATING		= 2;
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
		//exipration key should not be migrated
		return (zset_begin == "" && zset_end == "") || (zset_begin == EXPIRATION_LIST_KEY && zset_end == EXPIRATION_LIST_KEY);
	}
	bool empty() const{
		return kv_empty() && hash_empty() && queue_empty() && zset_empty();
	}
	std::string str() const{
		std::string buf;
		buf.append("begin:");
		buf.append(kv_begin);
		buf.append(" end:");
		buf.append(kv_end);
		return  buf;
	}
};

class Slot{
public:
	int id;
	SlotKeyRange range;
	Slot(){}
	Slot(int id, SlotKeyRange range){
		this->id=id;
		this->range=range;
	}
	Slot(const Slot &slot){
		this->id=slot.id;
		this->range=slot.range;
	}
};

class SSDB;

class SlotsManager
{
public:
	SlotsManager(SSDB *db, SSDB *meta, ExpirationHandler *expiration);
	~SlotsManager();
	SlotsManager(const SlotsManager &manager);

	//Slot List api
	int init_slots_list();
	int clear_slots_list();
	int get_slot(int id, Slot *slot);
	Slot* get_slot_ref(int id);
	int get_slot_list(std::vector<Slot> *ids_list);
	int get_slot_ids(std::vector<int> *list);
	int add_slot(int id, SlotKeyRange range);
	int del_slot(int id);

	//codis slot api 
	int slotsinfo(std::vector<int> *ids_list, int start=0, int count=HASH_SLOTS_SIZE);
	int slotsmgrtslot(std::string addr, int port, int timeout, int slot);
	int slotsmgrtstop();
	int slotsmgrtone(std::string addr, int port, int timeout, std::string name);

	//slot functions
	int slot_status(int slot_id);
	int slot_init(int slot_id);

private:
	SSDB *db;
	SSDB *meta;
	ExpirationHandler *expiration;
	SlotKeyRange load_slot_range(int id);
	std::string get_range_begin(int id, const char tyep);
	std::string get_range_end(int id, const char tyep);

	int set_slot_meta_status(int slot_id, const int status);
	int del_slot_meta_status(int slot_id);

	int slotsmgrtslot_kv(std::string addr, int port, int timeout, std::string name);
	int slotsmgrtslot_hash(std::string addr, int port, int timeout, std::string name);
	int slotsmgrtslot_queue(std::string addr, int port, int timeout, std::string name);
	int slotsmgrtslot_zset(std::string addr, int port, int timeout, std::string name);

	std::vector<Slot> slots_list;
	std::string slots_hash_key;
	Mutex mutex;

	struct run_arg{
		std::string addr;
		int port;
		int timeout;
		int slot_id;
		const SlotsManager *manager;
	};
	static void* _run_slotsmgrtslot(void *arg);
};

#endif
