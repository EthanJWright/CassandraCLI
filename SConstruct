env = Environment()
env.Program(target = 'show', source = ['show.cpp'], LIBS=['cassandra'])
env.Program(target = 'select', source = ['select.cpp'], LIBS=['cassandra'])
env.Program(target = 'cassandra', source = ['main.c', 'lex.c'], LIBS=['cassandra'])
