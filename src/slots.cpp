/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "slots.h"
//#include "ssdb/ssdb.h"
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

int SlotsManager::init_slots_list(){
	int i =0;
	for (i = 0; i < HASH_SLOTS_SIZE; ++i){
		SlotKeyRange krange = load_slot_range(i);
		if (!krange.empty()){
			log_debug("init_slots slot: %d ,key range %s", i, krange.kv_begin.c_str());
			add_slot(i, krange);
		}
	}
	return 0;
}

int SlotsManager::slot_status(int slot_id){
	std::string status;
	meta->hdel(slots_hash_key, str(slot_id));
	int ret = meta->hget(slots_hash_key, str(slot_id), &status);
	if (ret==1){
		log_debug("here %d", Bytes(status).Int());
		return Bytes(status).Int();
	}else {
		log_debug("here 2 %d", slot_init(slot_id));
		return slot_init(slot_id);
	}
	return -1;
}

int SlotsManager::slot_init(int slot_id){
	SlotKeyRange krange = load_slot_range(slot_id);
	if (!krange.empty()){
		int ret = meta->hset(slots_hash_key, str(slot_id), str(SlotStatus::NORMAL));
		if(ret == -1){
			log_error("slot %d init  error!", slot_id);
			return -1;
		}
		return 1;
	}else{
		int ret = meta->hset(slots_hash_key, str(slot_id), str(SlotStatus::EMPYT));
		if(ret == -1){
			log_error("slot %d init  error!", slot_id);
			return -1;
		}
		return 0;
	}
	return -1;
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

std::string SlotsManager::slotsinfo(){
	log_debug("Get slotsinfo begin");
	init_slots_list();
	std::string val;
	for(std::vector<Slot>::iterator it=slots_list.begin(); it!=slots_list.end(); it++){
		val.append("\n");
		val.append(str(it->id));
	}
	return val;
}

int SlotsManager::slotsmgrtslot(std::string addr, int port, int timeout, int slot_id){
	int ret = meta->hset(slots_hash_key, str(slot_id), str(SlotStatus::MIGRATING));
	if(ret == -1){
		log_error("slot %d migrate set status error!", slot_id);
		return -1;
	}
	pthread_t tid;
	struct run_arg *arg = new run_arg();
	arg->addr = addr;
	arg->port = port;
	arg->timeout = timeout;
	arg->slot_id = slot_id;
	arg->manager = new SlotsManager(this->db, this->meta);
	log_info("slots migrate slot %d begin", slot_id);
	int err = pthread_create(&tid, NULL, &SlotsManager::_run_slotsmgrtslot, arg);
	if(err != 0){
		fprintf(stderr, "can't create thread: %s\n", strerror(err));
	}
	return 0;
}

void* SlotsManager::_run_slotsmgrtslot(void *arg){
	pthread_detach(pthread_self());
	const char types[4] = {DataType::KV, DataType::HSIZE, DataType::QSIZE, DataType::ZSIZE};
	struct run_arg *p = (struct run_arg*)arg;
	std::string addr = p->addr;
	int port = p->port;
	int timeout = p->timeout;
	int slot_id = p->slot_id;
	SlotsManager *manager = (SlotsManager*)p->manager;
	log_info("migrate slot: %d ,to %s:%d begin", slot_id, addr.c_str(), port);
	for (int i = 0; i < sizeof(types); i++){
		std::string guard = slot_guard(types[i], (uint16_t)slot_id, "");
		Iterator *it;
		it = manager->db->iterator(guard, "", 1);
		while(it->next()){
			Bytes ks = it->key();
			if(ks.data()[0] == types[i]){
				std::string name;
				uint16_t slot;
				switch(types[i]){
				case DataType::KV:
					if(decode_kv_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						goto slot_empty;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_kv(addr, port, timeout, name);
					}else{
						goto slot_empty;
					}
					break;
				case DataType::HSIZE:
					if(decode_hsize_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						goto slot_empty;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_hash(addr, port, timeout, name);
					}else{
						goto slot_empty;
					}
					break;
				case DataType::ZSIZE:
					if(decode_zsize_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						goto slot_empty;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_queue(addr, port, timeout, name);
					}else{
						goto slot_empty;
					}
					break;
				case DataType::QSIZE:
					if(decode_qsize_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						goto slot_empty;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_zset(addr, port, timeout, name);
					}else{
						goto slot_empty;
					}
					break;
				}
			}
		}
	}
slot_empty:
	log_info("migrate slot: %d ,to %s:%d finished", slot_id, addr.c_str(), port);
	int ret2 = manager->meta->hdel(manager->slots_hash_key, str(slot_id));
	if(ret2 == -1){
		log_error("slot %d migrate set finished status error!", slot_id);
		//return -1;
	}
	delete(manager);
	return (void *)NULL;
}

int SlotsManager::slotsmgrtstop(){
	log_info("Slots migrate stop by hclear slots_hash_key");
	int ret = meta->hclear(slots_hash_key);
	if (ret == 0){
		return 0;
	}
	return 1;
}

int SlotsManager::slotsmgrtone(std::string addr, int port, int timeout, std::string name){
	int ret=1;
	if(slotsmgrtslot_kv(addr, port, timeout, name)==-1){
		ret = 0;
	}
	if (slotsmgrtslot_hash(addr, port, timeout, name)==-1){
		ret = 0;
	}
	if (slotsmgrtslot_queue(addr, port, timeout, name)==-1){
		ret = 0;
	}
	if (slotsmgrtslot_zset(addr, port, timeout, name)==-1){
		ret = 0;
	}
	return ret;
}

int SlotsManager::slotsmgrtslot_kv(std::string addr, int port, int timeout, std::string name){
	ssdb::Client *client = ssdb::Client::connect(addr, port);
	if(client == NULL){
		return -1;
	}
	std::string val;
	if (db->get(name, &val) < 0){
		log_error("get key %s error!", name.c_str());
	}
	ssdb::Status s;
	s = client->set(name, val);
	if(!s.ok()){
		log_error("dst server error! %s", s.code().c_str());
		return -1;
	}
	if (db->del(name) < 0){
		log_error("del key %s error!", name.c_str());
		return -1;
	}
	return 1;
}

int SlotsManager::slotsmgrtslot_hash(std::string addr, int port, int timeout, std::string name){
	ssdb::Client *client = ssdb::Client::connect(addr, port);
	if(client == NULL){
		return -1;
	}
	HIterator *it = db->hscan(name, "", "", 2000000000);
	ssdb::Status s;
	while(it->next()){
		s = client->hset(name, it->key, it->val);
		if(!s.ok()){
			log_error("dst server error! %s", s.code().c_str());
			return -1;
		}
	}
	if (db->hclear(name) < 0){
		log_error("del hash name %s error!", name.c_str());
		return -1;
	}
	return 1;
}

int SlotsManager::slotsmgrtslot_queue(std::string addr, int port, int timeout, std::string name){
	ssdb::Client *client = ssdb::Client::connect(addr, port);
	if(client == NULL){
		return -1;
	}
	int64_t count = 0;
	while(1){
		std::string item;
		int ret = db->qpop_front(name, &item);
		ssdb::Status s;
		s = client->qpush(name, item, &count);
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
	return 1;
}

int SlotsManager::slotsmgrtslot_zset(std::string addr, int port, int timeout, std::string name){
	ssdb::Client *client = ssdb::Client::connect(addr, port);
	if(client == NULL){
		return -1;
	}
	ZIterator *it = db->zrange(name, 0, 2000000000);
	ssdb::Status s;
	while(it->next()){
		//Bytes sc = new Bytes(it->score);
		s = client->zset(name, it->key, Bytes(it->score).Int64());
		if(!s.ok()){
			log_error("dst server error! %s", s.code().c_str());
			return -1;
		}
		if (db->zdel(name, it->key) < 0){
			log_error("del zset name %s key %s error!", name.c_str(), (it->key).c_str());
			return -1;
		}
	}
	return 1;
}

//SlotsManager private functions

SlotKeyRange SlotsManager::load_slot_range(int id){
	log_debug("Init slot %d", id);
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

