  
#include <string>
#include <cstdio>
#include <fstream>
#include <sstream>
#include <chrono>

#include <grpcpp/grpcpp.h>

#include "kvstore.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using namespace std;
using namespace std::chrono;

using kvstore::CommonReply;
using kvstore::DeleteRequest;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutRequest;

int mode; // 0 - interactive; 1 - batch; 2 - Performance Testing
long long int sum_of_duration = 0;
long long int min_response_time = INT_MAX;
long long int max_response_time = INT_MIN;
int num_threads = 1;
string LISTENING_PORT;
fstream logfs;

class KVClient
{
public:
	KVClient(shared_ptr<Channel> channel) : stub_(KVStore::NewStub(channel)) {}

	void set_max_min_response_time(long long int duration)
	{
		if (duration > max_response_time)
		{
			max_response_time = duration;
		}
		if (duration < min_response_time)
		{
			min_response_time = duration;
		}
	}

	void GET(string key)
	{
		GetRequest request;
		request.set_key(key);

		CommonReply reply;
		ClientContext context;

		auto start = high_resolution_clock::now();

		Status status = stub_->GET(&context, request, &reply);

		auto stop = high_resolution_clock::now();
		auto duration = duration_cast<microseconds>(stop - start);
		if (mode == 1)
		{
			sum_of_duration += duration.count();
			set_max_min_response_time(duration.count());
		}

		if (mode != 2)
		{
			if (reply.status() == 200)
			{
				cout << "Key " << key << " Value = " << reply.value() << " Status = " << reply.status() << endl;
			}
			else if (reply.status() == 400)
			{
				cout << "Status = " << reply.status() << " Error description = " << reply.errordescription() << endl;
			}

			cout << "Time taken by GET API: "
				 << duration.count() << " microseconds" << endl;

			if (status.ok())
			{
				//cout << "Key get successfully" << endl;
			}
			else
			{
				cout << "Connection Error.." << endl;
			}
		}
	}

	void DEL(string key)
	{
		DeleteRequest request;
		request.set_key(key);

		CommonReply reply;
		ClientContext context;

		auto start = high_resolution_clock::now();

		Status status = stub_->DEL(&context, request, &reply);

		auto stop = high_resolution_clock::now();
		auto duration = duration_cast<microseconds>(stop - start);
		if (mode == 1)
		{
			sum_of_duration += duration.count();
			set_max_min_response_time(duration.count());
		}

		if (mode != 2)
		{
			if (reply.status() == 200)
			{
				cout << "Status = " << reply.status() << endl;
			}
			else if (reply.status() == 400)
			{
				cout << "Status = " << reply.status() << " Error description = " << reply.errordescription() << endl;
			}

			cout << "Time taken by DELETE API: "
				 << duration.count() << " microseconds" << endl;

			if (status.ok())
			{
				//cout << "Key get successfully" << endl;
			}
			else
			{
				cout << "Connection Error.." << endl;
			}
		}
	}

	void PUT(string key, string value)
	{
		PutRequest request;
		request.set_key(key);
		request.set_value(value);

		CommonReply reply;
		ClientContext context;

		auto start = high_resolution_clock::now();

		Status status = stub_->PUT(&context, request, &reply);

		auto stop = high_resolution_clock::now();
		auto duration = duration_cast<microseconds>(stop - start);
		if (mode == 1)
		{
			sum_of_duration += duration.count();
			set_max_min_response_time(duration.count());
		}

		if (mode != 2)
		{
			if (reply.status() == 200)
			{
				cout << "Status = " << reply.status() << endl;
			}
			else if (reply.status() == 400)
			{
				cout << "ERROR in putting value" << endl;
			}

			cout << "Time taken by PUT API: "
				 << duration.count() << " microseconds" << endl;

			if (status.ok())
			{
				//cout << "Key get successfully" << endl;
			}
			else
			{
				cout << "Connection Error.." << endl;
			}
		}
	}

private:
	unique_ptr<KVStore::Stub> stub_;
};

char *argv_global;

void batchMode()
{

	string address="0.0.0.0:"+LISTENING_PORT;
	cout << address << endl;
	KVClient client(
		grpc::CreateChannel(
			address,
			grpc::InsecureChannelCredentials()));
	logfs << "CLIENT CONNECTED TO SERVER" << endl;
	mode = 1;
	fstream newfile;
	newfile.open(argv_global, ios::in);
	if (newfile.is_open())
	{
		string tp;
		double no_of_cmds = 0.0;
		while (getline(newfile, tp))
		{
			no_of_cmds++;
			cout << tp << "\n";
			string a[3];
			istringstream ss(tp);
			string del;
			int i = 0;
			while (getline(ss, del, ' '))
			{
				a[i] = del.c_str();
				i++;
			}
			if (a[1].length() > 256 || a[1].length() < 1)
			{
				cout << "KEY LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
			}
			else if (a[0].compare("GET") == 0)
			{
				client.GET(a[1]);
			}
			else if (a[0].compare("PUT") == 0)
			{
						if (a[2].length() > 256 || a[2].length() < 1)
			{
				cout << "VALUE LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
			}
				client.PUT(a[1], a[2]);
			}
			else if (a[0].compare("DEL") == 0)
			{
				client.DEL(a[1]);
			}
		}
		cout << "Timing Statistics - " << endl
			 << "========================================" << endl;
		cout << "Total duration = " << sum_of_duration << " microseconds" << endl;
		cout << "Average Time = " << sum_of_duration / no_of_cmds << " microseconds" << endl;
		cout << "Maximum response time = " << max_response_time << " microseconds" << endl;
		cout << "Minimum response time = " << min_response_time << " microseconds" << endl;
		newfile.close();
	}
}

void *multiBatchMode(void *vargp)
{

	long long int *sum = ((long long int *)vargp);
	//cout << endl << "Sum initial = " << sum <<  " " << *sum << endl;

	string address="0.0.0.0:"+LISTENING_PORT;
	KVClient client(
		grpc::CreateChannel(
			address,
			grpc::InsecureChannelCredentials()));
	logfs << "CLIENT CONNECTED TO SERVER" << endl;

	fstream newfile;
	newfile.open(argv_global, ios::in);
	if (newfile.is_open())
	{
		string tp;
		while (getline(newfile, tp))
		{
			//cout << tp << "\n";
			string a[3];
			istringstream ss(tp);
			string del;
			int i = 0;
			while (getline(ss, del, ' '))
			{
				a[i] = del.c_str();
				i++;
			}
			if (a[1].length() > 256 || a[1].length() < 1)
			{
				cout << "KEY LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
			}
			else if (a[0].compare("GET") == 0)
			{
				auto start = high_resolution_clock::now();
				client.GET(a[1]);
				auto stop = high_resolution_clock::now();
				auto duration = duration_cast<microseconds>(stop - start);
				*sum = *sum + duration.count();
			}
			else if (a[0].compare("PUT") == 0)
			{
				if (a[2].length() > 256 || a[2].length() < 1)
				{
					cout << "VALUE LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
				}
				auto start = high_resolution_clock::now();
				client.PUT(a[1], a[2]);
				auto stop = high_resolution_clock::now();
				auto duration = duration_cast<microseconds>(stop - start);
				*sum = *sum + duration.count();
			}
			else if (a[0].compare("DEL") == 0)
			{
				auto start = high_resolution_clock::now();
				client.DEL(a[1]);
				auto stop = high_resolution_clock::now();
				auto duration = duration_cast<microseconds>(stop - start);
				*sum = *sum + duration.count();
			}
		}
		newfile.close();
	}

	return NULL;
}

int main(int argc, char *argv[])
{

	/*string address("0.0.0.0:50051");
	KVClient client(
			grpc::CreateChannel(
				address, 
				grpc::InsecureChannelCredentials()
				)
	);*/
    logfs.open("../../log.txt", ios::app | ios::in);

	fstream config;
    config.open("../../config.txt", ios::in);
    if (config.is_open())
    {
        int i = 1;
        string tp;
        while (getline(config, tp))
        {
            if (i == 1)
			{
                LISTENING_PORT = tp;
				break;
			}
            i++;
        }
    }
	config.close();

	if (argc > 1)
	{
		cout << "Batch Mode : " << argv[1] << endl;
		argv_global = argv[1];
		if (num_threads == 1)
			batchMode();
		else
		{
			// fstream plot;
			// plot.open("../../plot.txt",ios::app);
			// for(int i=0;i<30;i++)
			// {
			mode = 2;
			pthread_t id[num_threads];
			long long int sum[num_threads];
			// long long int sum_times_all_threads = 0;
			long long int maxtime = 0;
			for (int i = 0; i < num_threads; i++)
			{
				sum[i] = 0;
				pthread_create(&id[i], NULL, multiBatchMode, (void *)&sum[i]);
			}

			for (int i = 0; i < num_threads; i++)
			{
				pthread_join(id[i], NULL);
			}
			maxtime=sum[0];
			for (int i = 0; i < num_threads; i++)
			{
				// cout << "Sum for thread id " << i << " = " << sum[i] << endl;
				// sum_times_all_threads += sum[i];
				if (maxtime<sum[i])
					maxtime=sum[i];
			}

			cout << "TOTAL SUM TEST MODE = " << maxtime << endl;
			// double rtime=double(maxtime/1000000.0);
			// double respone_time = sum_times_all_threads/(10000*num_threads);	// Avg Response Time = (Total Time/Total Requests)
			// plot << num_threads << " " << respone_time << " " << 10000.0*num_threads/rtime << endl;  // Throughput = Reqests/Total Time
			// num_threads++;
			// }
		}
	}
	else
	{
		string address="0.0.0.0:"+LISTENING_PORT;
		KVClient client(
			grpc::CreateChannel(
				address,
				grpc::InsecureChannelCredentials()));
	logfs << "CLIENT CONNECTED TO SERVER" << endl;

		string cmd, key, value;
		cout << "Interactive Mode : ";
		while (1)
		{
			cout << "$>";
			cin >> cmd;
			if (cmd.compare("GET") == 0)
			{
				cin >> key;
				if (key.length() > 256 || key.length() < 1)
				{
					cout << "KEY LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
				}
				else
				{
					client.GET(key);
				}
			}
			else if (cmd.compare("PUT") == 0)
			{
				cin >> key;
				cin >> value;
				if (key.length() > 256 || key.length() < 1)
				{
					cout << "KEY LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
				}
				else if (value.length() > 256 || value.length() < 1)
				{
					cout << "VALUE LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
				}
				else
					client.PUT(key, value);
			}
			else if (cmd.compare("DEL") == 0)
			{
				cin >> key;
				if (key.length() > 256 || key.length() < 1)
				{
					cout << "KEY LENGTH SHOULD BE BETWEEN 1 AND 256" << endl;
				}
				else
					client.DEL(key);
			}
			else if (cmd.compare("EXIT") == 0 || cmd.compare("exit") == 0)
				break;
		}
	}
    logfs.close();

	return 0;

	/*if(argc > 1){
		cout<<"Batch Mode started with input file: "<<argv[1] << endl;
		mode = 1;
		fstream newfile;
		newfile.open(argv[1],ios::in);
		if (newfile.is_open()){  
			string tp;
			double no_of_cmds = 0.0;
			while(getline(newfile, tp)){ 
				no_of_cmds++;
				cout << tp << "\n"; 
				string a[3];
				istringstream ss(tp);
				string del;
				int i=0;
				while(getline(ss,del,' ')){
					a[i]=del.c_str();  
					i++;
				}
				if(a[1].length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else if(a[0].compare("GET")==0){
					client.GET(a[1]);    
				}
				else if(a[0].compare("PUT")==0){
					client.PUT(a[1],a[2]);    
				}
				else if(a[0].compare("DEL")==0){
					client.DEL(a[1]);    
				}
			}
				cout << "Timing Statistics - " << endl << "========================================" << endl;
				cout << "Total duration = " << sum_of_duration << " microseconds" << endl;
				cout << "Average Time = " << sum_of_duration/no_of_cmds << " microseconds" << endl;
				cout << "Maximum response time = " << max_response_time << " microseconds" << endl;
				cout << "Minimum response time = " << min_response_time << " microseconds" << endl;
      			newfile.close();
		}
   
	}
	else{
		mode = 0;
		string cmd,key,value;
		cout<<"Interactive Mode started : " << endl;
		while(1){
			cout<< "$>" ;
			cin >> cmd;			
			if(cmd.compare("GET")==0){
				cin>>key;
				if(key.length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else {
					client.GET(key);
				}	
			}
			else if(cmd.compare("PUT")==0){
				cin>>key;
				cin>>value;
				if(key.length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else
					client.PUT(key,value);	
			}
			else if(cmd.compare("DEL")==0){
				cin>>key;
				if(key.length() > 256) {
					cout<< "KEY LENGTH CANNOT BE MORE THAN 256" << endl;
				}
				else
					client.DEL(key);	
			}
			else if(cmd.compare("EXIT")==0)
                		break;
		}
	}
	return 0;*/
}
