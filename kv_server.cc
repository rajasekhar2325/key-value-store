#include <iostream>
#include <memory>
#include <string>
#include <vector>
#include <string>
#include <deque>
#include <fcntl.h>
#include <fstream>
#include <cstring>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <unordered_map>
using namespace std;

#include <grpcpp/grpcpp.h>
#include "kvstore.grpc.pb.h"
#define files 20
using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::ServerReaderWriter;
using grpc::Status;

using kvstore::CommonReply;
using kvstore::DeleteRequest;
using kvstore::GetRequest;
using kvstore::KVStore;
using kvstore::PutRequest;

unordered_map<string, string> cache;
deque<string> lruqueue;
unordered_map<string, int> lfumap;
unordered_map<string, int> mdmap[20];
deque<int> delptr[20];
int fd[files];
string filename[files] = {"0.txt", "1.txt", "2.txt", "3.txt", "4.txt", "5.txt", "6.txt", "7.txt", "8.txt", "9.txt", "10.txt", "11.txt", "12.txt", "13.txt", "14.txt", "15.txt", "16.txt", "17.txt", "18.txt", "19.txt"};

pthread_rwlock_t rwlock[files];
unordered_map<string, pthread_rwlock_t> cacherwlock;
fstream logfs;

string LISTENING_PORT;
string CACHE_REPLACEMENT_TYPE;
int CACHE_SIZE;
int THREAD_POOL_SIZE;

int hashString(string s)
{
    int sum = 0;
    for (int i = 0; i < s.length(); i++)
    {
        sum += s[i];
    }
    return sum % 20;
}

void initmdmap(int fd[], int nof)
{
    for (int i = 0; i < nof; i++)
    {
        int filesize = lseek(fd[i], 0, SEEK_END);
        lseek(fd[i], 0, SEEK_SET);
        char buf[filesize];
        read(fd[i], buf, sizeof(buf));
        //         string str = buf;
        for (int j = 0; j < filesize; j += 513)
        {
            if (buf[j] == '1')
            {
                string key = "";
                for (int x = 1; x <= 256; x++)
                {
                    if (buf[j + x] == '\0')
                        break;
                    key += buf[j + x];
                }
                // cout << key << endl;
                mdmap[i][key] = j;
            }
            if (buf[j] == '0')
            {
                delptr[i].push_front(j);
            }
        }
    }
}

void put_in_file(string key, string value)
{

    int i = hashString(key);
    // cout << i << endl;
    int ptr;
    pthread_rwlock_wrlock(&rwlock[i]);
    if (mdmap[i].find(key) == mdmap[i].end())
    {
        if (delptr[i].empty())
        {
            ptr = lseek(fd[i], 0, SEEK_END);
            write(fd[i], "1", 1);
            write(fd[i], key.c_str(), 256);
            write(fd[i], value.c_str(), 256);
            mdmap[i][key] = ptr;
        }
        else
        {
            ptr = delptr[i].front();
            delptr[i].pop_front();
            lseek(fd[i], ptr, SEEK_SET);
            write(fd[i], "1", 1);
            write(fd[i], key.c_str(), 256);
            write(fd[i], value.c_str(), 256);
            mdmap[i][key] = ptr;
        }
    }
    else
    {
        lseek(fd[i], mdmap[i][key] + 257, SEEK_SET);
        write(fd[i], value.c_str(), 256);
        // cout << "alread present in store overwritten" << endl;
    }
    pthread_rwlock_unlock(&rwlock[i]);
}

string get_from_file(string key)
{

    int i = hashString(key);
    pthread_rwlock_rdlock(&rwlock[i]);
    if (mdmap[i].find(key) == mdmap[i].end())
    {
        pthread_rwlock_unlock(&rwlock[i]);
        return "";
    }
    int offset = mdmap[i][key]; //call map function
    lseek(fd[i], offset, SEEK_SET);
    char fkey[256], fvalue[256], fvalid[1];
    read(fd[i], fvalid, 1);
    read(fd[i], fkey, 256);
    read(fd[i], fvalue, 256);
    pthread_rwlock_unlock(&rwlock[i]);
    string fskey(fkey);
    string fsvalue(fvalue);
    if (key.compare(fskey) == 0)
    {
        return fsvalue;
    }
    return "";
}

int delete_from_file(string key)
{

    int i = hashString(key);
    pthread_rwlock_wrlock(&rwlock[i]);
    if (mdmap[i].find(key) == mdmap[i].end())
    {
        return -1;
    }
    int offset = mdmap[i][key]; //call map function
    lseek(fd[i], offset, SEEK_SET);
    write(fd[i], "0", 1);
    delptr[i].push_front(offset);
    mdmap[i].erase(key);
    pthread_rwlock_unlock(&rwlock[i]);
    return 1;
}

void initFD()
{
    for (int i = 0; i < files; i++)
    {
        fd[i] = open(filename[i].c_str(), O_CREAT | O_RDWR, S_IRWXU);
        if (fd < 0)
        {
            cout << "Cannot open filename" << filename[i];
        }
        pthread_rwlock_init(&rwlock[i], NULL);
    }
    initmdmap(fd, files);
    //################### Code For Locks ######################
}

void print_cache_in_log()
{
    logfs << endl
          << "CACHE CONTENTS::: ";
    for (auto i : cache)
        logfs << i.first << " ";
    logfs << endl;
}

void print_q_in_log()
{
    logfs << endl;
    for (auto i : lfumap)
        logfs << i.first << " " << i.second;
    logfs << endl;
    for (auto it = lruqueue.begin(); it != lruqueue.end(); ++it)
        logfs << ' ' << *it;
    logfs << endl;
}

string get_value(string key)
{

    logfs << "REQUEST: GET "
          << "PARAMETERS: " << key << " ";

    if (CACHE_REPLACEMENT_TYPE.compare("LRU") == 0)
    {

        if (cache.find(key) == cache.end()) // if key is not in cache
        {
            string value = get_from_file(key);
            if (value == "")
            {                                                   // if key is not in file return null
                logfs << "RETURN: NULL";
                print_cache_in_log();

                return "";
            }
            pthread_rwlock_init(&cacherwlock[key], NULL);
            if (cache.size() == CACHE_SIZE) // if cache is full pop the least frequent entry from cache and push current  key
            {
                pthread_rwlock_wrlock(&cacherwlock[key]);

                string last = lruqueue.back();
                lruqueue.pop_back();
                pthread_rwlock_wrlock(&cacherwlock[last]);
                cache.erase(last);
                pthread_rwlock_unlock(&cacherwlock[last]);
                cacherwlock.erase(last);
                cache[key] = value;
                lruqueue.push_front(key);

                pthread_rwlock_unlock(&cacherwlock[key]);
            }
            else                    // if cache is not full just push the current key in cache
            {
                pthread_rwlock_wrlock(&cacherwlock[key]);

                lruqueue.push_front(key);
                cache[key] = value;

                pthread_rwlock_unlock(&cacherwlock[key]);
            }
            logfs << "RETURN: " << value;
            print_cache_in_log();

            return value;
        }
        else                    // if key is in cache push it to ffront of the queue and return the value from cache
        {
            pthread_rwlock_wrlock(&cacherwlock[key]);

            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            lruqueue.erase(iter);
            lruqueue.push_front(key);
            logfs << "RETURN: " << cache[key];
            print_cache_in_log();

            pthread_rwlock_unlock(&cacherwlock[key]);

            pthread_rwlock_rdlock(&cacherwlock[key]);
            string temp = cache[key];
            pthread_rwlock_unlock(&cacherwlock[key]);
            return temp;
        }
    }

    if (CACHE_REPLACEMENT_TYPE.compare("LFU") == 0)
    {
        if (cache.find(key) == cache.end()) // if key is not in cache
        {
            string value = get_from_file(key);      
            if (value == "")                    // key not exists in file
            {
                logfs << "RETURN: NULL";
                print_cache_in_log();
                return "";
            }
            pthread_rwlock_init(&cacherwlock[key], NULL);
            if (CACHE_SIZE == lruqueue.size()) //  if cache size is full pop the least frequent entry from cache
            {
                int minfreq = 9999;
                for (auto i : lfumap)
                {
                    if (i.second < minfreq)
                        minfreq = i.second;
                }
                auto iter = lruqueue.rbegin();
                for (; iter != lruqueue.rend(); ++iter)
                    if (lfumap[*iter] == minfreq)
                        break;
                deque<string>::iterator it = lruqueue.begin();
                while (*it != *iter)
                    it++;

                pthread_rwlock_wrlock(&cacherwlock[*iter]);

                cache.erase(*iter);
                lfumap.erase(*iter);
                lruqueue.erase(it);

                pthread_rwlock_unlock(&cacherwlock[*iter]);
                cacherwlock.erase(*iter);
            }
            pthread_rwlock_wrlock(&cacherwlock[key]);       // push the new key in front of queue and insert in cache with frequency 1

            lruqueue.push_front(key);
            cache[key] = value;
            lfumap[key] = 1;

            pthread_rwlock_unlock(&cacherwlock[key]);

            logfs << "RETURN: " << value;
            print_cache_in_log();

            return value;
        }
        else
        {                                            // if key is already in cache, push it to front of queue and increase frequency by 1

            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;

            pthread_rwlock_wrlock(&cacherwlock[key]); 
            lruqueue.erase(iter);
            lruqueue.push_front(key);
            lfumap[key] = lfumap[key] + 1;
            pthread_rwlock_unlock(&cacherwlock[key]);

            logfs << "RETURN: " << cache[key];
            print_cache_in_log();

            pthread_rwlock_rdlock(&cacherwlock[key]);
            string temp = cache[key];
            pthread_rwlock_unlock(&cacherwlock[key]);

            return temp;
        }
    }

    return "";
}

void put_value(string key, string value)
{
    logfs << "REQUEST: PUT "
          << "PARAMETERS: " << key << "," << value << " ";

    if (CACHE_REPLACEMENT_TYPE.compare("LRU") == 0)
    {
        // logfs << "a\n";

        if (cache.find(key) == cache.end()) // if key is not present in cache
        {
            pthread_rwlock_init(&cacherwlock[key], NULL);

            if (CACHE_SIZE == lruqueue.size()) //  if cache size is full pop the last entry of cache
            {
                string last = lruqueue.back();
                pthread_rwlock_wrlock(&(cacherwlock[last]));
                lruqueue.pop_back();
                cache.erase(last);
                pthread_rwlock_unlock(&(cacherwlock[last]));
                cacherwlock.erase(last);
            }
        }
        else // else if key is in lru queue remove it and add it again in the front
        {
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            pthread_rwlock_wrlock(&(cacherwlock[key]));
            lruqueue.erase(iter);
            cache.erase(key);
            pthread_rwlock_unlock(&(cacherwlock[key]));
            cacherwlock.erase(key);
        }
        pthread_rwlock_wrlock(&(cacherwlock[key]));
        lruqueue.push_front(key);
        cache[key] = value;
        pthread_rwlock_unlock(&(cacherwlock[key]));
        put_in_file(key, value);
        logfs << "RESULT: PUT successful";
    }

    if (CACHE_REPLACEMENT_TYPE.compare("LFU") == 0)
    {

        if (cache.find(key) == cache.end()) // if key is not present in cache
        {
            pthread_rwlock_init(&cacherwlock[key], NULL);
            if (CACHE_SIZE == lruqueue.size()) // if cache size is full pop the least frequent entry from cache and push the new key to cache
            {
                int minfreq = 999999999;
                for (auto i : lfumap)
                {
                    if (i.second < minfreq)
                        minfreq = i.second;
                }
                auto iter = lruqueue.rbegin();
                for (; iter != lruqueue.rend(); ++iter)
                    if (lfumap[*iter] == minfreq)
                        break;
                deque<string>::iterator it = lruqueue.begin();
                while (*it != *iter)
                    it++;

                pthread_rwlock_wrlock(&(cacherwlock[*iter]));

                cache.erase(*iter);
                lfumap.erase(*iter);
                lruqueue.erase(it);

                pthread_rwlock_unlock(&(cacherwlock[*iter]));
                cacherwlock.erase(*iter);
            }

            pthread_rwlock_wrlock(&(cacherwlock[key]));
            lruqueue.push_front(key);
            cache[key] = value;
            lfumap[key] = 0;
            pthread_rwlock_unlock(&(cacherwlock[key]));
            put_in_file(key, value);
        }
        else // else if key is in cache increment frequency
        {
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            pthread_rwlock_wrlock(&(cacherwlock[key]));
            lruqueue.erase(iter);
            lruqueue.push_front(key);
            cache[key] = value;
            lfumap[key] += 1;
            pthread_rwlock_unlock(&(cacherwlock[key]));
            put_in_file(key, value);
        }
        logfs << "RESULT: PUT successful";
    }
    print_cache_in_log();
}

int delete_key(string key)
{
    logfs << "REQUEST: DEL "
          << "PARAMETER: " << key << " ";

    if (CACHE_REPLACEMENT_TYPE.compare("LRU") == 0)
    {
        if (cache.find(key) == cache.end())
        {
            int delstat = delete_from_file(key);
            if (delstat == -1)
            {
                logfs << "RESULT: KEY NOT EXIST IN CACHE OR FILE";
                print_cache_in_log();

                return 0;
            }
            else
            {
                logfs << "RESULT: KEY DELETED IN FILE";
                print_cache_in_log();

                return 1;
            }
        }
        else
        {
            pthread_rwlock_wrlock(&cacherwlock[key]);
            cache.erase(key);
            deque<string>::iterator it = lruqueue.begin();
            while (*it != key)
                it++;
            lruqueue.erase(it);
            pthread_rwlock_unlock(&cacherwlock[key]);
            cacherwlock.erase(key);
            delete_from_file(key);
            logfs << "RESULT: DELETED FROM CACHE and FILE";
            print_cache_in_log();

            return 1; // success
        }
    }

    if (CACHE_REPLACEMENT_TYPE.compare("LFU") == 0)
    {
        if (cache.find(key) == cache.end())             // if key not in cache
        {
            int delstat = delete_from_file(key);
            if (delstat == -1)
            {
                logfs << "RESULT: KEY NOT EXIST IN CACHE OR FILE";
                print_cache_in_log();

                return 0;
            }
            else
            {
                logfs << "RESULT: KEY DELETED IN FILE";
                print_cache_in_log();

                return 1;
            }
        }
        else                                                // if key is in cache erase it from cache and file
        {
            pthread_rwlock_wrlock(&cacherwlock[key]);
            cache.erase(key);
            deque<string>::iterator iter = lruqueue.begin();
            while (*iter != key)
                iter++;
            lruqueue.erase(iter);
            lfumap.erase(key);
            pthread_rwlock_unlock(&cacherwlock[key]);
            cacherwlock.erase(key);
            delete_from_file(key);
            logfs << "RESULT: DELETED FROM CACHE and FILE";
            print_cache_in_log();

            return 1; // success
        }
    }

    return 0;
}

int initialize()
{
    fstream newfile;
    newfile.open("../../config.txt", ios::in);
    if (newfile.is_open())
    {
        int i = 1;
        string tp;
        while (getline(newfile, tp))
        {
            if (i == 1)
                LISTENING_PORT = tp;
            else if (i == 2)
                CACHE_REPLACEMENT_TYPE = tp;
            else if (i == 3)
                CACHE_SIZE = stoi(tp);
            else if (i == 4)
                THREAD_POOL_SIZE = stoi(tp);
            i++;
        }
        cout << "INITIALIZING...." << endl;
        cout << "LISTENING PORT: " << LISTENING_PORT << endl;
        cout << "CACHE REPLACEMENT TYPE: " << CACHE_REPLACEMENT_TYPE << endl;
        cout << "CACHE SIZE: " << CACHE_SIZE << endl;
        cout << "Thread Pool Size: " << THREAD_POOL_SIZE << endl;

        logfs << "INITIALIZING...." << endl;
        logfs << "LISTENING PORT: " << LISTENING_PORT << endl;
        logfs << "CACHE REPLACEMENT TYPE: " << CACHE_REPLACEMENT_TYPE << endl;
        logfs << "CACHE SIZE: " << CACHE_SIZE << endl;
        logfs << "Thread Pool Size: " << THREAD_POOL_SIZE << endl;

        return 1;
    }
    return 0;
}

class ServerImpl final
{

    enum CallType
    {
        GET,
        DEL,
        PUT
    };
    CallType type_;

public:
    ~ServerImpl()
    {
        server_->Shutdown();
        cq_->Shutdown();
    }

    void Run()
    {
        std::string server_address= "0.0.0.0:"+LISTENING_PORT;

        ServerBuilder builder;
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        builder.RegisterService(&service_);
        cq_ = builder.AddCompletionQueue();
        grpc::ResourceQuota rq;
        rq.SetMaxThreads(THREAD_POOL_SIZE);
        builder.SetResourceQuota(rq);

        server_ = builder.BuildAndStart();

        logfs << "Server Started: listening on " << server_address << std::endl;
        std::cout << "Server listening on " << server_address << std::endl;

        HandleRpcs();
    }

private:
    class CallData
    {

    public:
        CallData(KVStore::AsyncService *service, ServerCompletionQueue *cq, CallType ctype)
            : service_(service), cq_(cq), responder_(&ctx_), status_(CREATE), type_(ctype)
        {
            Proceed();
        }
    
        void Proceed()
        {
            if (status_ == CREATE)
            {
                //logfs << "New Connection Established With Client..." << endl;

                status_ = PROCESS;

                if (type_ == GET)
                    service_->RequestGET(&ctx_, &request_, &responder_, cq_, cq_,
                                         this);
                if (type_ == PUT)
                    service_->RequestPUT(&ctx_, &putrequest_, &responder_, cq_, cq_,
                                         this);

                if (type_ == DEL)
                    service_->RequestDEL(&ctx_, &deleterequest_, &responder_, cq_, cq_,
                                         this);
            }
            else if (status_ == PROCESS)
            {

                if (type_ == GET)
                {
                    new CallData(service_, cq_, GET);

                    string key = request_.key();
                    string value = get_value(key);
                    if (value.compare("") == 0)
                    {
                        reply_.set_status(400);
                        reply_.set_errordescription("KEY NOT EXIST");
                    }
                    else
                    {
                        reply_.set_status(200);
                        reply_.set_value(value);
                        reply_.set_errordescription("GET SUCCESFULL");
                    }

                    status_ = FINISH;
                    responder_.Finish(reply_, Status::OK, this);
                }

                else if (type_ == PUT)
                {
                    new CallData(service_, cq_, PUT);

                    string key = putrequest_.key();
                    string value = putrequest_.value();
                    put_value(key, value);
                    reply_.set_status(200);
                    reply_.set_errordescription("PUT SUCCESFULL");

                    status_ = FINISH;
                    responder_.Finish(reply_, Status::OK, this);
                }

                else if (type_ == DEL)
                {
                    new CallData(service_, cq_, DEL);

                    string key = deleterequest_.key();
                    int value = delete_key(key);
                    if (value == 0)
                    {
                        reply_.set_status(400);
                        reply_.set_errordescription("KEY NOT EXIST");
                    }
                    else
                    {
                        reply_.set_status(200);
                        reply_.set_errordescription("KEY DELETED");
                    }

                    status_ = FINISH;
                    responder_.Finish(reply_, Status::OK, this);
                }
            }
            else
            {
                GPR_ASSERT(status_ == FINISH);
                delete this;
            }
        }

    private:
        KVStore::AsyncService *service_;
        ServerCompletionQueue *cq_;
        ServerContext ctx_;

        GetRequest request_;
        CommonReply reply_;
        PutRequest putrequest_;
        DeleteRequest deleterequest_;

        ServerAsyncResponseWriter<CommonReply> responder_;

        enum CallStatus
        {
            CREATE,
            PROCESS,
            FINISH
        };
        CallStatus status_; // The current serving state.
        CallType type_;
    };

    void HandleRpcs()
    {
        //ServerImpl::CallType typex;
        new CallData(&service_, cq_.get(), GET);
        new CallData(&service_, cq_.get(), PUT);
        new CallData(&service_, cq_.get(), DEL);
        void *tag; // uniquely identifies a request.
        bool ok;
        while (true)
        {
            GPR_ASSERT(cq_->Next(&tag, &ok));
            GPR_ASSERT(ok);
            static_cast<CallData *>(tag)->Proceed();
        }
    }

    std::unique_ptr<ServerCompletionQueue> cq_;
    KVStore::AsyncService service_;
    std::unique_ptr<Server> server_;
};

int main(int argc, char **argv)
{

    logfs.open("../../log.txt", ios::app | ios::in);

    int t = initialize();
    if (t == 0)
    {
        cout << "Cannot Open Config File" << endl;
        return 0;
    }
    initFD();

    ServerImpl server;
    server.Run();

    logfs.close();

    return 0;
}
