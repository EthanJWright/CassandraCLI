#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <vector>
#include <sstream>
#include <cassandra.h>
#include <iterator>
#include <algorithm>


using namespace std;

string keyspace = "not set";
string table = "not set";
char* hosts = (char *)"127.0.0.1";
/* Setup and connect to cluster */
CassFuture* connect_future = NULL;
CassCluster* cluster = NULL;
CassSession* session = NULL;
CassFuture* close_future = NULL;

void setup(){
  cluster = cass_cluster_new();
  session = cass_session_new();
  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);
  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);
}
void error(CassFuture* future){
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(future, &message, &message_length);
    fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
}
void close(){
  /* Close the session */
  close_future = cass_session_close(session);
  cass_future_wait(close_future);
  cass_future_free(close_future);
}

void show(){
  setup();
  cout<<"Keyspace List: "<<endl;
  cout<<"----------------"<<endl;
  if (cass_future_error_code(connect_future) == CASS_OK) {
    /* Build statement and execute query */
    const char* query = "select * from system_schema.keyspaces";
    CassStatement* statement = cass_statement_new(query, 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);
      CassIterator* iterator = cass_iterator_from_result(result);

      while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        string response(release_version);
        cout<<response<<endl;
      }
      cass_result_free(result);
    } else {
      error(result_future);
    }
    cass_statement_free(statement);
    cass_future_free(result_future);
    close();
  } else {
    error(connect_future);
  }
}

void list(){
  setup();
  cout<<"Tables: "<<endl;
  cout<<"-----------------"<<endl;
  if (cass_future_error_code(connect_future) == CASS_OK) {
    /* Build statement and execute query */
    if(keyspace == "not set"){
      cout<<"Keyspace not defined"<<endl;
      return;
    }
    string query = "SELECT table_name FROM system_schema.columns WHERE keyspace_name = '" + keyspace + "'";
    CassStatement* statement = cass_statement_new(query.c_str(), 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);
      CassIterator* iterator = cass_iterator_from_result(result);

      std::vector<string> table_names;

      while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        const CassValue* value = cass_row_get_column_by_name(row, "table_name");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        string name(release_version);
        bool found = std::find(table_names.begin(), table_names.end(), name) != table_names.end();
        if(!found){
          table_names.push_back(name);
        }
      }
      for(int i = 0; i < table_names.size(); i++){
            // Create iterator pointing to first element
        std::vector<std::string>::iterator it = table_names.begin();
        std::advance(it, i);
        cout << *it << endl;
      }
      cass_result_free(result);
    } else {
      error(result_future);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);
    close();
  } else {
    error(connect_future);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
}
void use(string key, string tab){
  keyspace = key;
  table = tab;
  string response = "Using keyspace: " + keyspace + " and table: " + table;
  cout<<response<<endl;
}
void get(string key){
  setup();
  cout<<"Values for key: "<<endl;
  cout<<"--------------"<<endl;
  if (cass_future_error_code(connect_future) == CASS_OK) {
    /* Build statement and execute query */
    string key = "first";
    string query = "SELECT " + key + " FROM " + keyspace + "." + table;
    CassStatement* statement = cass_statement_new(query.c_str(), 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);
      CassIterator* iterator = cass_iterator_from_result(result);

      while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        const CassValue* value = cass_row_get_column_by_name(row, key.c_str());

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        string ret_value(release_version);
        cout<<ret_value<<endl;
      }
      cass_result_free(result);
    } else {
      error(result_future);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);
    close();
  } else {
    error(connect_future);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
}
void insert(string key, string value){
  setup();
  if (cass_future_error_code(connect_future) == CASS_OK) {
    /* Build statement and execute query */
    string query = "INSERT INTO " + keyspace + "." + table + " ( " + key + " ) VALUES ( '" + value + "' )";
    CassStatement* statement = cass_statement_new(query.c_str(), 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);
      CassIterator* iterator = cass_iterator_from_result(result);

      while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        const CassValue* value = cass_row_get_column_by_name(row, "keyspace_name");

        const char* release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        string response(release_version);
        cout<<response<<endl;
      }
      cass_result_free(result);
    } else {
      error(result_future);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);
    close();
  } else {
    error(connect_future);
  }
  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);
}


int main(int argc, char ** argv)
{
    while(1)
    {
        setup();
        string prompt = ">";
        if(keyspace != "not set"){
          prompt = keyspace + ">";
        }
        char * line = readline(prompt.c_str());
        if(!line) break;
        if(*line) add_history(line);
        string entered(line);

        std::string s = entered;
        std::vector<std::string> result;
        std::istringstream iss(s);
        // Split into vector by white space
        for(std::string s; iss >> s; )
              result.push_back(s);

        if(result.size() == 1){
          if(result[0] == "quit" || result[0] == "exit" || result[0] == "q"){
            exit(0);
          }
          if(result[0] == "show"){
            show();
          }
          if(result[0] == "list"){
            list();
          }
        }if(result.size() == 2){
            if(result[0] == "list"){
              if(keyspace == "not set"){
                keyspace = result[1];
                list();
                keyspace = "not set";
              }
          }
          if(result[0] == "get"){
            get(result[0]);
          }
          if(result[0] == "use"){
            // Split with the '.'
            stringstream ss(result[1]);
            vector<string> split;
            while( ss.good() )
            {
              string substr;
              getline( ss, substr, '.' );
              split.push_back( substr );
            }
            // call use
            use(split[0], split[1]);
          }
        }if(result.size() == 3){
          if(result[0] == "insert"){
            insert(result[1], result[2]);
          }
        }
        free(line);
    }
}
