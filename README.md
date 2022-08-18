Mayank Taneja 214050005
Mohan Rajasekhar Ajjampudi 213050060
Arance Kurmi 213050056


To Build the code for the first time, run the following commands in the kvstore folder, so that the cmake folder and all dependencies get created - 

$ mkdir -p cmake/build
$ pushd cmake/build
$ cmake -DCMAKE_PREFIX_PATH=$MY_INSTALL_DIR ../.. 
$ make

For later builds, just run the "make" command in the cmake/build directory inside kvstore folder

After building, two binaries will be generated - kv_client for client and kv_server for server in the cmake/build folder.
To run the code, go to cmake/build folder and run the command  "./kv_client" for running client and "./kv_server" for server

Provide desired parameters through config.txt file present in the kvstore directory in the following order (each in new line) :
	1. LISTENING_PORT
	2. CACHE_REPLACEMENT_TYPE (type LRU or LFU)
	3. CACHE_SIZE
	4. THREAD_POOL_SIZE

Format of Requests - 
	//To get a key
	GET <key>
	//To put a key
	PUT <key> <value>
	//To delete a key
	DEL <key>
	

For running the client in batch mode, use the command ./kv_client {filename}, where filename is the name of the test file containing the requests in each line. The file should be present in the same folder as kv_client binary or provide the full path of the file

For running the client in interactive mode, do not provide filename parameter, just run ./kv_client
 
The server logs are getting recorded in the file log.txt present in kvstore folder

The Performance Analysis Graphs and observations are in "Performance Analysis.pdf" file

