#include "WorkerNode.h"

// Worker Node Parameterized Constructor
WorkerNode::WorkerNode(std::string node_name){
	this->node_name = node_name;
	db_filename = node_name + "_syslogs.db";
	
	int status = sqlite3_open(db_filename.c_str(), &this->db);
	
	if(status == SQLITE_OK){
		char* error;
		int res = sqlite3_exec(
			this->db, 																							// sqlite3 db database
			"CREATE TABLE syslogs (date TEXT, timestamp TEXT, hostname TEXT, daemon TEXT, severity INT, message TEXT)", // sql statement
			NULL, 																							// callback function
			0,																								// 1st parameter to callback function
			&error																							// error message
		);
		
		if(res == SQLITE_OK){
			std::cout << this->node_name + " Database Successfully Created.\n";
		} else{
			std::cout << error << "\n";
			sqlite3_free(error);
		}
	}
}

WorkerNode::~WorkerNode(){
	sqlite3_close(this->db);
}

// Write Operations
grpc::Status WorkerNode::Ingest(grpc::ServerContext* context, grpc::ServerReader<LogString>* reader, RequestStatus* response){
	return grpc::Status::OK;
}

grpc::Status WorkerNode::Purge(grpc::ServerContext* context, PurgeRequest*, RequestStatus* response){
	return grpc::Status::OK;
}

// Read Operations
grpc::Status SearchDate(grpc::ServerContext* context, const QueryRequest* request, LogResults* response){
	char* error;
	std::string statement = "SELECT * FROM syslogs WHERE date = ?";
	sqlite3_stmt* prepared_statement;
	
	// create a prepared statement
	int res = sqlite3_prepare(
		this->db,				// pointer to the sqlite3 db
		statement,				// source sql statement
		statement.length(),		// length of sql statement characters
		&prepared_statement,	// prepared statement output
		nullptr					// unsure how this field is computed, but it's the pointer to unused portion of the source statement
	);

	if(res != SQLITE_OK){
		return grpc::Status::FAILED_PRECONDITION; // operation was rejected because the system is not in a state required for the operation’s execution
	}

	// bind the prepared statement with the QueryRequest argument field
	res = sqlite3_bind_text(
		prepared_statement,				// prepared statement
		1,								// index of argument in prepared statement
		request->argument().c_str(),	// argument field from QueryRequest in protobuf -> convert to C char*
		-1,								// length -1 to automatically compute length
		SQLITE_TRANSIENT 				// instead of SQLITE_STATIC to indicate value may change in the future
	);

	if(res != SQLITE_OK){
		sqlite3_finalize(prepared_statement); //cleanup statement
		return grpc::Status::FAILED_PRECONDITION; // operation was rejected because the system is not in a state required for the operation’s execution
	}
		
	while(sqlite3_step(prepared_statement) == SQLITE_ROW){
		const char* text = reinterpret_cast<const char*>(sqlite3_column_text(prepared_statement, 0));
	}
	
	sqlite3_finalize(prepared_statement); // cleanup statement
	
	return grpc::Status::OK;
}