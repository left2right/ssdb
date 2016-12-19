/*
Copyright (c) 2012-2015 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "slots.h"
#include "util/log.h"
#include "ssdb/t_kv.h"
#include "ssdb/t_hash.h"
#include "ssdb/t_zset.h"
#include "ssdb/t_queue.h"
#include "SSDB_client.h"

SlotsManager::SlotsManager(SSDB *db, SSDB *meta, ExpirationHandler *expiration){
	this->db = db;
	this->meta = meta;
	this->expiration = expiration;
	this->slots_hash_key="SLOTS_HASH";
}

SlotsManager::~SlotsManager(){
	db = NULL;
	meta = NULL;
	expiration = NULL;
}

//slot api
int SlotsManager::init_slots_list(){
	clear_slots_list();
	int i =0;
	for (i = 0; i < HASH_SLOTS_SIZE; ++i){
		SlotKeyRange krange = load_slot_range(i);
		if (!krange.empty()){
			log_info("slot: %d exists", i);
			add_slot(i, krange);
		}
	}
	return 0;
}

int SlotsManager::clear_slots_list(){
	std::vector<Slot>().swap(slots_list);
	return 0;
}

int SlotsManager::add_slot(int id, SlotKeyRange krange){
	Slot slot;
	slot.id = id;
	slot.range = krange;
	slots_list.push_back(slot);
	return slot.id;
}

int SlotsManager::slot_status(int slot_id){
	log_info("get slot %d status", slot_id);
	std::string status;
	int ret = meta->hget(slots_hash_key, str(slot_id), &status);
	if (ret >= 1){
		return Bytes(status).Int();
	}else {
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
		log_info("slot %d empty", slot_id);
		return 0;
	}
	return -1;
}

//codis slot api
int SlotsManager::slotsinfo(std::vector<int> *ids_list, int start, int count){
	if (count == 1){
		int ret = this->slot_status(start);
		ids_list->push_back(start);
		ids_list->push_back(ret);
		return 0;
	}

	init_slots_list();
	std::vector<int> tmp_ids;
	for(std::vector<Slot>::iterator it=slots_list.begin(); it!=slots_list.end(); it++){
		tmp_ids.push_back(it->id);
	}
	std::sort(tmp_ids.begin(), tmp_ids.end());
	int temp_count = 0;
	for(std::vector<int>::iterator it=tmp_ids.begin(); (it!=tmp_ids.end())&&(temp_count <count); it++){
		if (*it >= start){
			ids_list->push_back(*it);
			ids_list->push_back(1);
		}	
		temp_count++;
	}
	return 0;
}

int SlotsManager::slotsmgrtslot(std::string addr, int port, int timeout, int slot_id){
	pthread_t tid;
	struct run_arg *arg = new run_arg();
	arg->addr = addr;
	arg->port = port;
	arg->timeout = timeout;
	arg->slot_id = slot_id;
	arg->manager = this;
	int err = pthread_create(&tid, NULL, &SlotsManager::_run_slotsmgrtslot, arg);
	if(err != 0){
		log_error("can't create thread: %s\n", strerror(err));
		return -1;
	}
	int ret = meta->hset(slots_hash_key, str(slot_id), str(SlotStatus::MIGRATING));
	if(ret == -1){
		log_error("slot %d migrate set status error!", slot_id);
		return -1;
	}
	return 0;
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

int SlotsManager::slotsmgrtstop(){
	log_info("Slots migrate stop by hclear slots_hash_key");
	int ret = meta->hclear(slots_hash_key);
	if (ret == 0){
		return 0;
	}
	return 1;
}

//SlotsManager private functions
SlotKeyRange SlotsManager::load_slot_range(int id){
	log_debug("Get slot %d key range", id);
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

int SlotsManager::set_slot_meta_status(int slot_id, const int status){
	int ret = this->meta->hset(slots_hash_key, str(slot_id), str(status));
	if(ret == -1){
		log_error("set slot %d meta status error!", slot_id);
		return -1;
	}
	return 0;
}

int SlotsManager::del_slot_meta_status(int slot_id){
	int ret = this->meta->hdel(slots_hash_key, str(slot_id));
	if(ret == -1){
		log_error("hdel slot %d meta status error!", slot_id);
		return -1;
	}
	log_debug("del slot %d meta status", slot_id);
	return 0;
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
	int64_t ttl = this->expiration->get_ttl(name);
	ssdb::Status s;
	if (ttl == -1){
		s = client->set(name, val);
		if(!s.ok()){
			log_error("dst server error! %s", s.code().c_str());
			return -1;
		}
	}else{
		s = client->setx(name, val, ttl);
		if(!s.ok()){
			log_error("dst server error! %s", s.code().c_str());
			return -1;
		}
	}
	
	if (db->del(name) < 0){
		log_error("del key %s error!", name.c_str());
		return -1;
	}
	log_info("migrate kv key: %s ,to %s:%d", name.c_str(), addr.c_str(), port);
	delete client;
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
	log_info("migrate hash key: %s ,to %s:%d", name.c_str(), addr.c_str(), port);
	delete client;
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
		if(ret == 0){
			break;
		}
		if(ret == -1){
			return -1;
		}
		ssdb::Status s;
		s = client->qpush(name, item, &count);
		if(!s.ok()){
			log_error("dst server error! %s", s.code().c_str());
			return -1;
		}
	}
	log_info("migrate queue key: %s ,to %s:%d", name.c_str(), addr.c_str(), port);
	delete client;
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
	log_info("migrate zset key: %s ,to %s:%d", name.c_str(), addr.c_str(), port);
	delete client;
	return 1;
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
	Locking l(&manager->mutex);

	for (int i = 0; i < sizeof(types); i++){
		std::string guard = slot_guard(types[i], (uint16_t)slot_id, "");
		Iterator *it;
		it = manager->db->iterator(guard, "", 100000);
		while(it->next()){
			Bytes ks = it->key();
			if(ks.data()[0] == types[i]){
				std::string name;
				uint16_t slot;
				switch(types[i]){
				case DataType::KV:
					if(decode_kv_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						manager->set_slot_meta_status(slot_id, SlotStatus::NORMAL);
						return (void *)NULL;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_kv(addr, port, timeout, name);
					}else{
						goto loop_types;
					}
					break;
				case DataType::HSIZE:
					if(decode_hsize_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						manager->set_slot_meta_status(slot_id, SlotStatus::NORMAL);
						return (void *)NULL;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_hash(addr, port, timeout, name);
					}else{
						goto loop_types;
					}
					break;
				case DataType::QSIZE:
					if(decode_zsize_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						manager->set_slot_meta_status(slot_id, SlotStatus::NORMAL);
						return (void *)NULL;
					}
					if(slot == slot_id){
						manager->slotsmgrtslot_queue(addr, port, timeout, name);
					}else{
						goto loop_types;
					}
					break;
				case DataType::ZSIZE:
					if(decode_qsize_key(ks, &name, &slot) == -1){
						log_error("migrate slot at decode key error");
						manager->set_slot_meta_status(slot_id, SlotStatus::NORMAL);
						return (void *)NULL;
					}

					//do not migrate expiration key EXPIRATION_LIST_KEY
					if (name == EXPIRATION_LIST_KEY){
						break;
					}

					if(slot == slot_id){
						manager->slotsmgrtslot_zset(addr, port, timeout, name);
					}else{
						log_info("slotsmgrtslot migrate slot: %d ,to %s:%d finished", slot_id, addr.c_str(), port);
						manager->del_slot_meta_status(slot_id);
						return (void *)NULL;
					}
					break;
				}
			}else {
				goto loop_types;
			}
		}
		loop_types:
			log_debug("slotsmgrtslot migrate slot %d type %c finished", slot_id, types[i]);
	}
	manager->del_slot_meta_status(slot_id);
	return (void *)NULL;

}

