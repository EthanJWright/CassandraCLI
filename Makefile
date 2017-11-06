all:
	gcc -o cassandra lex.c main.c -lcassandra

clean:
	rm -f cassandra
