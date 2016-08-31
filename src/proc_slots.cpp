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

	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	std::string info = manager->slotsinfo();
	delete(manager);

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

	log_debug("Slots %d migrate to %s:%d", slot_id, addr.c_str(), port);

	std::string val;
	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slot_status(slot_id);
	//delete(manager);

	switch(ret){
	case 0: 
	case -1:
		log_debug("Slots %d migrate to %s:%d, but not running with status %d", slot_id, addr.c_str(), port, ret);
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back("0");
		break;
	case 1:
		log_debug("Slots %d migrate to %s:%d begin!", slot_id, addr.c_str(), port);
		manager->slotsmgrtslot(addr, port, timeout, slot_id);
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		break;
	case 2:
		log_debug("Slots %d migrate to %s:%d running!", slot_id, addr.c_str(), port);
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
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
	log_debug("Migrate %s to %s:%d", name.c_str(), addr.c_str(), port);

	SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
	int ret = manager->slotsmgrtone(addr, port, timeout, name);
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

int proc_slotsmgrtstop(NetworkServer *net, Link *link, const Request &req, Response *resp){
	CHECK_NUM_PARAMS(1);

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
	return 0;
}

int proc_slotsmgrttagone(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return 0;
}

int proc_slotsrestore(NetworkServer *net, Link *link, const Request &req, Response *resp){
	return 0;
}






