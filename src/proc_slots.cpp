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
	manager->init_slots();

	std::vector<int> ids_list;
	manager->get_slot_ids(&ids_list);
	resp->push_back("ok");
	std::vector<int>::iterator it;
	for(it=ids_list.begin(); it!=ids_list.end(); it++){
		resp->add(*it);
		//resp->add(1);
	}

	delete(manager);
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

	std::string val;
	int ret = serv->meta->hget("SLOTS_HASH", str(slot_id), &val);
	if(ret <= 0){
		SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
		//manager->init_slots();
		manager->migrate_slot(addr, port, timeout, slot_id);
		int ret = serv->meta->hget("SLOTS_HASH", str(slot_id), &val);
		delete(manager);
		if (ret <= 0){
			resp->push_back("ok");
			resp->push_back("0");
			resp->push_back("0");
			return 0;
		}else {
			resp->push_back("ok");
			resp->push_back("1");
			resp->push_back("1");
			return 0;
		}
	}

	if (val == str(0)){
		log_debug("slots migrate 1");
		SlotsManager *manager = new SlotsManager(serv->ssdb, serv->meta);
		//manager->init_slots();
		manager->migrate_slot(addr, port, timeout, slot_id);
		log_debug("slots migrate 2");
		delete(manager);
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		return 0;
	}else if (val == str(1))
	{
		resp->push_back("ok");
		resp->push_back("1");
		resp->push_back("1");
		return 0;
	}else {
		resp->push_back("ok");
		resp->push_back("0");
		resp->push_back("0");
		return 0;
	}
	return 0;
}

int proc_slotsmgrtone(NetworkServer *net, Link *link, const Request &req, Response *resp){
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






