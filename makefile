src=$(wildcard ./*.c)
obj=$(patsubst ./%.c, ./%.o, $(src))

target=main
CC=gcc

$(target):$(obj)
	$(CC) $^ -o $@

%.o:%.c
	$(CC) -c $< -o $@

.PHONY:clean
clean:
	rm $(obj) $(target) -f


