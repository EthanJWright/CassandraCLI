env = Environment()
env.Program(target = 'show', source = ['show.cpp'], LIBS=['cassandra'])
env.Program(target = 'select', source = ['select.cpp'], LIBS=['cassandra'])
env.Program(target = 'c_list', source = ['c_list.cpp'], LIBS=['cassandra'])
env.Program(target = 'cassandra', source = ['main.c', 'lex.c'], LIBS=['cassandra'])
env.Program(target = 'insert', source = ['insert.cpp'], LIBS=['cassandra'])
env.Program(target = 'get', source = ['get.cpp'], LIBS=['cassandra'])


env.Program(target = 'cli', source = ['cli_main.cpp'], LIBS=['cassandra', 'readline'])
