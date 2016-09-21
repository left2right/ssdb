/************************************************* 
Copyright:
Author: left2right
Date: 2016-09-21 
Description: ssdb as codis server, needed commands:
		config, slaveof, slotshashkey, slotsinfo,
		slotsmgrtslot, slotsmgrtone, slotsmgrttagslot,
		slotsmgrttagone, slotsmgrtstop
**************************************************/ 
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"

int proc_config(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(3);
	CHECK_KV_KEY_RANGE(1);

	std::string operate = req[1].String();
	strtolower(&operate);
	std::string key = req[2].String();
	strtolower(&key);	
	if (operate == "get")
	{
		resp->push_back("ok");
		resp->push_back(key);
		resp->push_back("0");
	}else{
		resp->push_back("error");
	}
	return 0;
}

int proc_slaveof(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(3);

	resp->push_back("ok");
	return 0;
}

int proc_slotshashkey(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(2);

	log_info("slotshashkey get key hash slot value");

	resp->push_back("ok");
	for(int i=1; i<req.size(); i++){
		int ret = (int)req[i].slots();
		resp->add(ret);
	}
	return 0;
}

int proc_slotsinfo(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(1);
	SSDBServer *serv = (SSDBServer *)net->data;

	SlotsManager *manager = serv->slots_manager;
	std::vector<int> slots_list;
	if (req.size() == 3){
		log_info("slotsinfo request 3, %s %d, %d", req[0].String().c_str(), req[1].Int(), req[2].Int());
		int ret = manager->slotsinfo(&slots_list, req[1].Int(), req[2].Int());
		if (ret != 0){
			resp->push_back("error");
			return -1;
		}
	}else if (req.size() == 2){
		log_info("slotsinfo request 2, %s %d", req[0].String().c_str(), req[1].Int());
		int ret = manager->slotsinfo(&slots_list, req[1].Int());
		if (ret != 0){
			resp->push_back("error");
			return -1;
		}
	}else {
		log_info("slotsinfo request 1, %s", req[0].String().c_str());
		int ret = manager->slotsinfo(&slots_list);
		if (ret != 0){
			resp->push_back("error");
			return -1;
		}
	}
	
	resp->push_back("ok");
	for(std::vector<int>::iterator it=slots_list.begin(); it!=slots_list.end(); it++){
		resp->add(*it);
	}
	return 0;
}

int proc_slotsmgrtslot(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string addr = req[1].String();
	int port = req[2].Int();
	int timeout = req[3].Int();
	int slot_id = req[4].Int();

	//log_info("slotsmgrtslot migrate slot %d to %s:%d", slot_id, addr.c_str(), port);

	std::string val;
	SlotsManager *manager = serv->slots_manager;
	int ret = manager->slot_status(slot_id);

	int ret2;
	switch(ret){
	case 0: 
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back("0");
		log_info("slotsmgrtslot migrate slot %d to %s:%d, but slot is empty, status 0", slot_id, addr.c_str(), port);
		break;
	case -1:
		resp->push_back("error");
		log_error("slotsmgrtslot migrate slot %d to %s:%d exception, status -1", slot_id, addr.c_str(), port);
		break;
	case 1:
		ret2 = manager->slotsmgrtslot(addr, port, timeout, slot_id);
		if (ret2 == -1){
			resp->push_back("error");
			return 0;
		}
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		log_info("slotsmgrtslot migrate slot %d to %s:%d, begin, status 1", slot_id, addr.c_str(), port);
		break;
	case 2:
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		log_info("slotsmgrtslot migrate slot %d to %s:%d running, status 2!", slot_id, addr.c_str(), port);
		break;	
	}
	return 0;
}

int proc_slotsmgrtone(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string addr = req[1].String();
	int port = req[2].Int();
	int timeout = req[3].Int();
	std::string name = req[4].String();
	log_info("slotsmgrtone migrate  %s to %s:%d begin", name.c_str(), addr.c_str(), port);

	SlotsManager *manager = serv->slots_manager;
	int ret = manager->slotsmgrtone(addr, port, timeout, name);

	resp->push_back("ok");
	if (ret==1)
	{
		log_info("slotsmgrtone migrate  %s to %s:%d finish", name.c_str(), addr.c_str(), port);
		resp->push_back("1");
	}else{
		log_info("slotsmgrtone migrate  %s to %s:%d, but key not exists", name.c_str(), addr.c_str(), port);
		resp->push_back("0");
	}
	
	return 0;
}

int proc_slotsmgrttagslot(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string addr = req[1].String();
	int port = req[2].Int();
	int timeout = req[3].Int();
	int slot_id = req[4].Int();

	//log_info("slotsmgrtslot migrate slot %d to %s:%d", slot_id, addr.c_str(), port);

	std::string val;
	SlotsManager *manager = serv->slots_manager;
	int ret = manager->slot_status(slot_id);

	int ret2;
	switch(ret){
	case 0: 
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back("0");
		log_info("slotsmgrtslot migrate slot %d to %s:%d, but slot is empty, status 0", slot_id, addr.c_str(), port);		
		break;
	case -1:
		resp->push_back("error");
		log_error("slotsmgrtslot migrate slot %d to %s:%d exception, status -1", slot_id, addr.c_str(), port);
		break;
	case 1:
		ret2 = manager->slotsmgrtslot(addr, port, timeout, slot_id);
		if (ret2 == -1){
			resp->push_back("error");
			return 0;
		}
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		log_info("slotsmgrtslot migrate slot %d to %s:%d, begin ,status 1", slot_id, addr.c_str(), port);
		break;
	case 2:
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		log_info("slotsmgrtslot migrate slot %d to %s:%d running, status 2", slot_id, addr.c_str(), port);
		break;	
	}
	return 0;
}

int proc_slotsmgrttagone(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string addr = req[1].String();
	int port = req[2].Int();
	int timeout = req[3].Int();
	std::string name = req[4].String();
	log_info("slotsmgrtone migrate  %s to %s:%d begin", name.c_str(), addr.c_str(), port);

	SlotsManager *manager = serv->slots_manager;
	int ret = manager->slotsmgrtone(addr, port, timeout, name);

	resp->push_back("ok");
	if (ret==1)
	{
		log_info("slotsmgrtone migrate  %s to %s:%d finish", name.c_str(), addr.c_str(), port);
		resp->push_back("1");
	}else{
		log_info("slotsmgrtone migrate  %s to %s:%d, but key not exists", name.c_str(), addr.c_str(), port);
		resp->push_back("0");
	}
	
	return 0;
}

int proc_slotsmgrtstop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(1);

	log_info("slotsmgrtstop stop migrating, just hclear SLOTS_HASH meta key, more work to do");
	SSDBServer *serv = (SSDBServer *)net->data;
	SlotsManager *manager = serv->slots_manager;
	int ret = manager->slotsmgrtstop();

	resp->push_back("ok");
	if (ret==1)
	{
		resp->push_back("1");
	}else{
		resp->push_back("0");
	}
	
	return 0;
}

int proc_slavedecoder(NetworkServer *net, Link *link, const Request &req, Response *resp){
	SSDBServer *serv = (SSDBServer *)net->data;
	CHECK_NUM_PARAMS(1);

	std::string decoder;
	if (req.size() >= 3 && req[1].String() == "set"){
		decoder = req[2].String();
		strtolower(&decoder);	
		if (decoder == "ssdb" || decoder == "slot"){
			for (std::vector<Slave *>::iterator it= (serv->slaves).begin(); it != (serv->slaves).end(); it++){
				(*it)->decoder = decoder;
			}
			log_info("set slave decoder %s", decoder.c_str());
			resp->push_back("ok");
			resp->push_back(decoder);
			return 0;
		}else{
			log_error("slave_decoder wrong type %s", decoder.c_str());
			resp->push_back("error");
			return -1;
		}
	}

	resp->push_back("ok");
	if (!(serv->slaves.empty())){
		decoder = serv->slaves.front()->decoder;
		resp->push_back(decoder);
	}
	
	return 0;
} 

