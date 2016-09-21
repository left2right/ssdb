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

