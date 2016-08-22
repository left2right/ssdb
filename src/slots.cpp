/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "slots.h"
#include "ssdb/ssdb.h"
#include "util/log.h"
#include "ssdb/t_kv.h"
#include "ssdb/t_hash.h"
#include "ssdb/t_zset.h"
#include "ssdb/t_queue.h"
#include "SSDB_client.h"

std::string SlotKeyRange::str() const{
		std::string buf;
		buf.append("begin:");
		buf.append(kv_begin);
		buf.append(" end:");
		buf.append(kv_end);
		return  buf;
}

SlotsManager::SlotsManager(SSDB *db, SSDB *meta){
	log_debug("SlotsManager init");
	this->db = db;
	this->meta = meta;
	this->slots_hash_key="SLOTS_HASH";
}

SlotsManager::~SlotsManager(){
	log_debug("SlotsManager finalized");
}

int SlotsManager::init_slots(){
	log_debug("slots migrate 4");
	int i =0;
	for (i = 0; i < HASH_SLOTS_SIZE; ++i){
		SlotKeyRange krange = load_slot_range(i);
		if (!krange.empty()){
			log_debug("init_slots slot: %d ,key range %s", i, krange.kv_begin.c_str());
			add_slot(i, krange);
			int ret = meta->hset(slots_hash_key, str(i), str(SlotStatus::NORMAL));
			if(ret == -1){
				log_error("cluster store error!");
				return -1;
			}
		}
	}
	return 0;
}

int SlotsManager::get_slot(int id, Slot *ret){
	Locking l(&mutex);
	Slot *slot = this->get_slot_ref(id);
	if(slot){
		*ret = *slot;
		return 1;
	}
	return 0;
}

std::string SlotsManager::slotsinfo(){
	std::string val;
	std::vector<Slot>::iterator it;
	for(it=slots_list.begin(); it!=slots_list.end(); it++){
		log_debug("slot: %d ,key range %s to %s", it->id, it->range.kv_begin.c_str(), it->range.kv_end.c_str());
		val.append(str(it->id));
		val.append(":");
		val.append(it->range.kv_begin);
		val.append("-");
		val.append(it->range.kv_end);
		val.append("\n");
	}
	return val;
}

Slot* SlotsManager::get_slot_ref(int id){
	std::vector<Slot>::iterator it;
	for(it=slots_list.begin(); it!=slots_list.end(); it++){
		Slot &slot = *it;
		if(slot.id == id){
			return &slot;
		}
	}
	return NULL;
}

int SlotsManager::get_slot_list(std::vector<Slot> *list){
	Locking l(&mutex);
	*list = slots_list;
	return 0;
}

int SlotsManager::get_slot_ids(std::vector<int> *ids_list){
	std::vector<Slot>::iterator it;
	for(it=slots_list.begin(); it!=slots_list.end(); it++){
		ids_list->push_back(it->id);
	}
	return 0;
}

int SlotsManager::add_slot(int id, SlotKeyRange krange){
	Slot slot;
	slot.id = id;
	slot.range = krange;
	slots_list.push_back(slot);
	return slot.id;
}

int SlotsManager::del_slot(int id){
	Locking l(&mutex);
	std::vector<Slot>::iterator it;
	for(it=slots_list.begin(); it!=slots_list.end(); it++){
		const Slot &slot = *it;
		if(slot.id == id){
			slots_list.erase(it);
			return 1;
		}
	}
	return 0;
}

int SlotsManager::migrate_slot(std::string addr, int port, int timeout, int slot_id){
	init_slots();
	migrate_slot_keys(addr, port, timeout, slot_id);
	return 0;
}

int SlotsManager::migrate_slot_keys(std::string addr, int port, int timeout, int slot_id){
	ssdb::Client *client = ssdb::Client::connect(addr, port);
	if(client == NULL){
		return -1;
	}
	Slot *slot = get_slot_ref(slot_id);
	if (!slot->range.kv_empty())
	{
		std::string val;
		if (db->get(slot->range.kv_begin, &val) < 0){
			log_error("get key %s error!", slot->range.kv_begin.c_str());
		}
		ssdb::Status s;
		s = client->set(slot->range.kv_begin, val);
		if(!s.ok()){
			log_error("dst server error! %s", s.code().c_str());
			return -1;
		}
		if (db->del(slot->range.kv_begin) < 0){
			log_error("del key %s error!", slot->range.kv_begin.c_str());
			return -1;
		}
		return 1;
	}else if (!slot->range.hash_empty())
	{
		HIterator *it = db->hscan(slot->range.hash_begin, "", "", 2000000000);
		ssdb::Status s;
		while(it->next()){
			s = client->hset(slot->range.hash_begin, it->key, it->val);
			if(!s.ok()){
				log_error("dst server error! %s", s.code().c_str());
				return -1;
			}
		}
		if (db->hclear(slot->range.hash_begin) < 0){
			log_error("del hash name %s error!", slot->range.hash_begin.c_str());
			return -1;
		}
		return 1;
	}else if (!slot->range.queue_empty())
	{
		int64_t count = 0;
		while(1){
			std::string item;
			int ret = db->qpop_front(slot->range.queue_begin, &item);
			ssdb::Status s;
			s = client->qpush(slot->range.queue_begin, item, &count);
			if(!s.ok()){
				log_error("dst server error! %s", s.code().c_str());
				return -1;
			}
			if(ret == 0){
				break;
			}
			if(ret == -1){
				return -1;
			}
		}
	}else if (!slot->range.zset_empty())
	{
		ZIterator *it = db->zrange(slot->range.zset_begin, 0, 2000000000);
		ssdb::Status s;
		while(it->next()){
			//Bytes sc = new Bytes(it->score);
			s = client->zset(slot->range.zset_begin, it->key, Bytes(it->score).Int64());
			if(!s.ok()){
				log_error("dst server error! %s", s.code().c_str());
				return -1;
			}
			if (db->zdel(slot->range.zset_begin, it->key) < 0){
				log_error("del zset name %s key %s error!", slot->range.zset_begin.c_str(), (it->key).c_str());
				return -1;
			}
		}
		return 1;
	}	
	return 0;
}

//SlotsManager private functions

SlotKeyRange SlotsManager::load_slot_range(int id){
	SlotKeyRange range;
	range.kv_begin = get_range_begin(id, DataType::KV);
	range.kv_end = get_range_end(id, DataType::KV);
	range.hash_begin = get_range_begin(id, DataType::HSIZE);
	range.hash_end = get_range_end(id, DataType::HSIZE);
	range.queue_begin = get_range_begin(id, DataType::QSIZE);
	range.queue_end = get_range_end(id, DataType::QSIZE);
	range.zset_begin = get_range_begin(id, DataType::ZSIZE);
	range.zset_end = get_range_end(id, DataType::ZSIZE);
	return range;
}

std::string SlotsManager::get_range_begin(int id, const char type){
	std::string guard = slot_guard(type, (uint16_t)id, "");
	Iterator *it;
	it = db->iterator(guard, "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == type){
			std::string n;
			uint16_t slot;
			switch(type){
			case DataType::KV:
				if(decode_kv_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			case DataType::HSIZE:
				if(decode_hsize_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			case DataType::ZSIZE:
				if(decode_zsize_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			case DataType::QSIZE:
				if(decode_qsize_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			}
		}
	}
	delete it;	
	return "";
}

std::string SlotsManager::get_range_end(int id, const char type){
	std::string guard = slot_guard(type, (uint16_t)id, "\xff");
	Iterator *it;
	it = db->rev_iterator(guard, "", 1);
	if(it->next()){
		Bytes ks = it->key();
		if(ks.data()[0] == type){
			std::string n;
			uint16_t slot;
			switch(type){
			case DataType::KV:
				if(decode_kv_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			case DataType::HSIZE:
				if(decode_hsize_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			case DataType::ZSIZE:
				if(decode_zsize_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			case DataType::QSIZE:
				if(decode_qsize_key(ks, &n, &slot) == -1){
					return "";
				}else if(slot == id){
					return n;
				}
				break;
			}
		}
	}
	delete it;	
	return "";
}

