src=$(wildcard ./*.c)
obj=$(patsubst ./%.c, ./%.o, $(src))

target=main
CC=gcc

$(target):$(obj)
	$(CC) $^ -lpthread -o $@

%.o:%.c
	$(CC) -lpthread -c $< -o $@

.PHONY:clean
clean:
	rm $(obj) $(target) -f


