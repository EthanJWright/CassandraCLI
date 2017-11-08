#include <cassandra.h>
#include <stdio.h>
#include <string.h>
#include <iostream>


using namespace std;

string keyspace = "excelsior";
string table = "test";

int main(int argc, char* argv[]) {
  /* Setup and connect to cluster */
  CassFuture* connect_future = NULL;
  CassCluster* cluster = cass_cluster_new();
  CassSession* session = cass_session_new();
  char* hosts = "127.0.0.1";
  if (argc > 1) {
    hosts = argv[1];
  }

  /* Add contact points */
  cass_cluster_set_contact_points(cluster, hosts);

  /* Provide the cluster object as configuration to connect the session */
  connect_future = cass_session_connect(session, cluster);

  if (cass_future_error_code(connect_future) == CASS_OK) {
    CassFuture* close_future = NULL;

    /* Build statement and execute query */
//    const char* query = "SELECT release_version FROM system.local";
    string query = "select * from " + keyspace + "." + table;
    CassStatement* statement = cass_statement_new(query.c_str(), 0);

    CassFuture* result_future = cass_session_execute(session, statement);

    if (cass_future_error_code(result_future) == CASS_OK) {
      /* Retrieve result set and get the first row */
      const CassResult* result = cass_future_get_result(result_future);
      const CassRow* row = cass_result_first_row(result);
      CassIterator* iterator = cass_iterator_from_result(result);

      while (cass_iterator_next(iterator)) {
        const CassRow* row = cass_iterator_get_row(iterator);
        CassIterator* val_iter = cass_iterator_from_row(row);
        while(cass_iterator_next(val_iter)){
          const CassValue* value = cass_iterator_get_column(val_iter);

          const char* val_char;
          size_t val_length;
          cass_value_get_string(value, &val_char, &val_length);


          const char* c_string;
          size_t c_string_length;
          cass_value_get_string(value, &c_string, &c_string_length);
          string str_val(c_string);
          printf("%s", str_val.c_str());
          printf(" ,");
        }
        printf("\n");

      }
      cass_result_free(result);
    } else {
      /* Handle error */
      const char* message;
      size_t message_length;
      cass_future_error_message(result_future, &message, &message_length);
      fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
    }

    cass_statement_free(statement);
    cass_future_free(result_future);

    /* Close the session */
    close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
  } else {
    /* Handle error */
    const char* message;
    size_t message_length;
    cass_future_error_message(connect_future, &message, &message_length);
    fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
  }

  cass_future_free(connect_future);
  cass_cluster_free(cluster);
  cass_session_free(session);

  return 0;
}