/*
Copyright (c) 2012-2014 The SSDB Authors. All rights reserved.
Use of this source code is governed by a BSD-style license that can be
found in the LICENSE file.
*/
#include "worker.h"
#include "link.h"
#include "proc.h"
#include "../util/log.h"
#include "../include.h"

ProcWorker::ProcWorker(const std::string &name){
	this->name = name;
}

void ProcWorker::init(){
	log_debug("%s %d init", this->name.c_str(), this->id);
}
/*
抽象了具体的执行任务过程：
	step1 . 根据ProcJob获取对应的Command，执行其中的任务处理函数
    step2 . 将任务结果放到Link的输出缓冲区中
*/
int ProcWorker::proc(ProcJob *job){
	const Request *req = job->req;
	
	proc_t p = job->cmd->proc;
	// 任务等待的时长
	job->time_wait = 1000 * (millitime() - job->stime);
	// 根据传入的处理函数处理请求，得到结果
	job->result = (*p)(job->serv, job->link, *req, &job->resp);
	// 任务处理的时长
	job->time_proc = 1000 * (millitime() - job->stime) - job->time_wait;
	// 任务结果放到Link输出缓冲区中
	if(job->link->send(job->resp.resp) == -1){
		job->result = PROC_ERROR;
	}else{
		if(job->cmd->flags & Command::FLAG_READ){
			int len = job->link->write();
			if(len < 0){
				job->result = PROC_ERROR;
			}
		}
	}
	return 0;
}
