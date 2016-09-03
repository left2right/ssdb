/* codis */
#include "serv.h"
#include "net/proc.h"
#include "net/server.h"
//#include "slots.h"

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

	log_info("slotsinfo get slots in this ssdb");

	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	std::string info;
	int ret = manager->slotsinfo(info);
	delete(manager);
	if (ret != 0){
		resp->push_back("error");
		return -1;
	}

	resp->push_back("ok");
	resp->push_back(info);
	return 0;
}

int proc_slotsdel(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return 0;
}

int proc_slotsmgrtslot(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(4);
	SSDBServer *serv = (SSDBServer *)net->data;

	std::string addr = req[1].String();
	int port = req[2].Int();
	int timeout = req[3].Int();
	int slot_id = req[4].Int();

	log_info("slotsmgrtslot migrate slot %d to %s:%d", slot_id, addr.c_str(), port);

	std::string val;
	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slot_status(slot_id);

	int ret2;
	switch(ret){
	case 0: 
		log_info("slotsmgrtslot migrate slot %d to %s:%d, but slot is empty", slot_id, addr.c_str(), port);
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back("0");
		break;
	case -1:
		log_error("slotsmgrtslot migrate slot %d to %s:%d exception", slot_id, addr.c_str(), port);
		resp->push_back("error");
		break;
	case 1:
		log_info("slotsmgrtslot migrate slot %d to %s:%d begin!", slot_id, addr.c_str(), port);
		ret2 = manager->slotsmgrtslot(addr, port, timeout, slot_id);
		if (ret2 == -1){
			resp->push_back("error");
			return 0;
		}
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		break;
	case 2:
		log_info("slotsmgrtslot migrate slot %d to %s:%d running!", slot_id, addr.c_str(), port);
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		break;	
	}
	delete(manager);
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

	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slotsmgrtone(addr, port, timeout, name);
	delete(manager);

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
	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slotsmgrtstop();
	delete(manager);

	resp->push_back("ok");
	if (ret==1)
	{
		resp->push_back("1");
	}else{
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

	log_info("slotsmgrtslot migrate slot %d to %s:%d", slot_id, addr.c_str(), port);

	std::string val;
	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slot_status(slot_id);

	int ret2;
	switch(ret){
	case 0: 
		log_info("slotsmgrtslot migrate slot %d to %s:%d, but slot is empty", slot_id, addr.c_str(), port);
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back("0");
		break;
	case -1:
		log_error("slotsmgrtslot migrate slot %d to %s:%d exception", slot_id, addr.c_str(), port);
		resp->push_back("error");
		break;
	case 1:
		log_info("slotsmgrtslot migrate slot %d to %s:%d begin!", slot_id, addr.c_str(), port);
		ret2 = manager->slotsmgrtslot(addr, port, timeout, slot_id);
		if (ret2 == -1){
			resp->push_back("error");
			return 0;
		}
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		break;
	case 2:
		log_info("slotsmgrtslot migrate slot %d to %s:%d running!", slot_id, addr.c_str(), port);
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		break;	
	}
	delete(manager);
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

	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slotsmgrtone(addr, port, timeout, name);
	delete(manager);

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

int proc_slotsrestore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return 0;
}






