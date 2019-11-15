src=$(wildcard ./*.c)

CC=gcc

obj1=main.o BPTree.o db.o
obj2=server.o BPTree.o db.o

all:main server client

main:$(obj1)
	$(CC) $^ -lpthread -o $@

server:$(obj2)
	$(CC) $^ -lpthread -o $@

client:client.o
	$(CC) $^ -lpthread -o $@

%.o:%.c
	$(CC) -c $<  -o $@

.PHONY:clean
clean:
	rm $(obj1) $(obj2) main server -f
