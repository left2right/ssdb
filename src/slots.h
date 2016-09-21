/************************************************* 
Copyright:
Author: left2right
Date: 2016-09-21 
Description: ssdb slots manager
**************************************************/ 
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

inline static
std::string slot_guard(const char type, uint16_t id, const char *key){
	std::string guard;
	guard.append(1, type);
	uint16_t slot = big_endian(id);
	guard.append((char *)&slot, sizeof(uint16_t));
	guard.append(key, 1);
	return guard;
}

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

	SlotKeyRange(){}

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
		return zset_begin == "" && zset_end == "";
	}
	bool empty() const{
		return kv_empty() && hash_empty() && queue_empty() && zset_empty();
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

	//Slot List api
	int init_slots_list();
	int clear_slots_list();
	int add_slot(int id, SlotKeyRange range);

private:
	SSDB *db;
	SSDB *meta;
	ExpirationHandler *expiration;
	SlotKeyRange load_slot_range(int id);
	std::string get_range_begin(int id, const char tyep);
	std::string get_range_end(int id, const char tyep);

	std::vector<Slot> slots_list;
	std::string slots_hash_key;
};

#endif
