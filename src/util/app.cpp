#include "app.h"
#include "log.h"
#include "file.h"
#include "config.h"
#include "daemon.h"
#include "strings.h"
#include <stdio.h>

int Application::main(int argc, char **argv){
	conf = NULL;

	welcome();
	parse_args(argc, argv);
	//init函数完成整个初始化流程
	init();

	write_pid();
	//调用派生类实现的run函数启动具体功能
	run();
	//进程退出时删除pidfile
	remove_pidfile();
	
	delete conf;
	return 0;
}

void Application::usage(int argc, char **argv){
	printf("Usage:\n");
	printf("    %s [-d] /path/to/app.conf [-s start|stop|restart]\n", argv[0]);
	printf("Options:\n");
	printf("    -d    run as daemon\n");
	printf("    -s    option to start|stop|restart the server\n");
	printf("    -h    show this message\n");
}

/*
	1. 解析是否是守护进程
	2. 解析启动操作是start | stop | restart
	3. 解析配置文件名
*/
void Application::parse_args(int argc, char **argv){
	for(int i=1; i<argc; i++){
		std::string arg = argv[i];
		if(arg == "-d"){
			app_args.is_daemon = true;
		}else if(arg == "-v"){
			exit(0);
		}else if(arg == "-h"){
			usage(argc, argv);
			exit(0);
		}else if(arg == "-s"){
			if(argc > i + 1){
				i ++;
				app_args.start_opt = argv[i];
			}else{
				usage(argc, argv);
				exit(1);
			}
			if(app_args.start_opt != "start" && app_args.start_opt != "stop" && app_args.start_opt != "restart"){
				usage(argc, argv);
				fprintf(stderr, "Error: bad argument: '%s'\n", app_args.start_opt.c_str());
				exit(1);
			}
		}else{
			app_args.conf_file = argv[i];
		}
	}

	if(app_args.conf_file.empty()){
		usage(argc, argv);
		exit(1);
	}
}
/*
init函数完成整个初始化流程
 	1. 导入配置文件(配置文件路径通过parse_args解析命令行参数获得)
    2. 根据配置的目录，修改当前进程的工作目录
    3. 如果参数为"restart"或"stop"，则重启或终止服务进程，重启服务进程就是杀死已经存在服务进程然后继续执行，然后当前进程成为新的服务进程。
    4. 检查是否存在保存pid的文件，如果存在，则说明服务进程还在运行（杀死服务进程会删除pid 文件），当前进程异常退出。
    5. 打开记录日志。
    6. 根据daemon参数判断是否需要变成守护进程（注意：这一步必须在创建任何线程之前执行）。
*/
void Application::init(){
	if(!is_file(app_args.conf_file.c_str())){
		fprintf(stderr, "'%s' is not a file or not exists!\n", app_args.conf_file.c_str());
		exit(1);
	}
	//解析配置文件到conf
	conf = Config::load(app_args.conf_file.c_str());
	if(!conf){
		fprintf(stderr, "error loading conf file: '%s'\n", app_args.conf_file.c_str());
		exit(1);
	}
	{
		std::string conf_dir = real_dirname(app_args.conf_file.c_str());
		if(chdir(conf_dir.c_str()) == -1){
			fprintf(stderr, "error chdir: %s\n", conf_dir.c_str());
			exit(1);
		}
	}
	// 获取配置的pidfile文件路径
	app_args.pidfile = conf->get_str("pidfile");

	// 如果参数为stop，则杀死服务进程，然后自己退出
	if(app_args.start_opt == "stop"){
		kill_process();
		exit(0);
	}
	// 如果参数为restart，则杀死服务进程，自己继续执行
	if(app_args.start_opt == "restart"){
		if(file_exists(app_args.pidfile)){
			kill_process();
		}
	}
	// 检查pid文件是否存在，判断服务进程是否被杀死
	check_pidfile();
	// 根据日志配置打开日志功能
	{ // logger
		std::string log_output;
		std::string log_level_;
		int64_t log_rotate_size;

		log_level_ = conf->get_str("logger.level");
		strtolower(&log_level_);
		if(log_level_.empty()){
			log_level_ = "debug";
		}
		int level = Logger::get_level(log_level_.c_str());
		log_rotate_size = conf->get_int64("logger.rotate.size");
		log_output = conf->get_str("logger.output");
		if(log_output == ""){
			log_output = "stdout";
		}
		if(log_open(log_output.c_str(), level, true, log_rotate_size) == -1){
			fprintf(stderr, "error opening log file: %s\n", log_output.c_str());
			exit(1);
		}
	}

	app_args.work_dir = conf->get_str("work_dir");
	if(app_args.work_dir.empty()){
		app_args.work_dir = ".";
	}
	if(!is_dir(app_args.work_dir.c_str())){
		fprintf(stderr, "'%s' is not a directory or not exists!\n", app_args.work_dir.c_str());
		exit(1);
	}

	// WARN!!!
	// deamonize() MUST be called before any thread is created!
	if(app_args.is_daemon){
		daemonize();
	}
}

int Application::read_pid(){
	if(app_args.pidfile.empty()){
		return -1;
	}
	std::string s;
	file_get_contents(app_args.pidfile, &s);
	if(s.empty()){
		return -1;
	}
	return str_to_int(s);
}

void Application::write_pid(){
	if(app_args.pidfile.empty()){
		return;
	}
	int pid = (int)getpid();
	std::string s = str(pid);
	int ret = file_put_contents(app_args.pidfile, s);
	if(ret == -1){
		log_error("Failed to write pidfile '%s'(%s)", app_args.pidfile.c_str(), strerror(errno));
		exit(1);
	}
}

void Application::check_pidfile(){
	if(app_args.pidfile.size()){
		if(access(app_args.pidfile.c_str(), F_OK) == 0){
			fprintf(stderr, "Fatal error!\nPidfile %s already exists!\n"
				"Kill the running process before you run this command,\n"
				"or use '-s restart' option to restart the server.\n",
				app_args.pidfile.c_str());
			exit(1);
		}
	}
}

void Application::remove_pidfile(){
	if(app_args.pidfile.size()){
		remove(app_args.pidfile.c_str());
	}
}

/*
杀死服务进程的流程：发送SIGTERM信号，服务进程在收到SIGTERM信号后会调用注册的signal_handle，然后将quit置为true，最后退出事件循环。
*/
void Application::kill_process(){
	// 从pid_file中读取正在运行的APP的pid
	int pid = read_pid();
	if(pid == -1){
		fprintf(stderr, "could not read pidfile: %s(%s)\n", app_args.pidfile.c_str(), strerror(errno));
		exit(1);
	}
	// 检查进程是否存在
	if(kill(pid, 0) == -1 && errno == ESRCH){
		fprintf(stderr, "process: %d not running\n", pid);
		remove_pidfile();
		return;
	}
	// server在收到SIGTERM信号会结束事件循环，见\src\net\server.cpp 中signal_handler函数
	// 结束事件循环导致Application::main函数中调用的run函数退出，然后remove_pidfile删除文件
	int ret = kill(pid, SIGTERM);
	if(ret == -1){
		fprintf(stderr, "could not kill process: %d(%s)\n", pid, strerror(errno));
		exit(1);
	}
	// 如果pidfile还存在，说明被杀死的服务进程还没有退出，继续等待。
	while(file_exists(app_args.pidfile)){
		usleep(100 * 1000);
	}
}

