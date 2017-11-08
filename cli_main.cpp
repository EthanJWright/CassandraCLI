#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <vector>
#include <sstream>

using namespace std;

void show(){
  cout<<"showing"<<endl;
}
void list(){
  cout<<"listing"<<endl;
}
void use(string keyspace, string table){
  string response = "Using keyspace: " + keyspace + " and table: " + table;
  cout<<response<<endl;
}
void get(){
  cout<<"getting"<<endl;
}
void insert(){
  cout<<"inserting"<<endl;
}


int main(int argc, char ** argv)
{
    while(1)
    {
        char * line = readline("> ");
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
          if(result[0] == "get"){
            get();
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
            insert();
          }
        }
        /* Do something witcons the line here */
        free(line);
    }
}
