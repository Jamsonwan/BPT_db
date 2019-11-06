#include "db.h"

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>

// id 为文件的索引， fd为文件描述符
static off_t get_offset(int id, int fd, int type){
	off_t endPosition;
	int off_set = id * RECORD_LEN;
	if (type == READ){
		endPosition = lseek(fd, off_set, SEEK_SET);
	}else{
		endPosition = lseek(fd, off_set, SEEK_SET);
	}
	return endPosition;
}

// id 为文件的索引，fd为文件描述符
extern int Update(int id, int fd, Data newData){
	printf("id %d\n", id);

	printf("%d %s %d %c\n", newData.age, newData.name, newData.std_no, newData.sex);
	Data oldData = Select(id, fd);

	printf("%d %s %d %c\n", oldData.age, oldData.name, oldData.std_no, oldData.sex);
	if (newData.age < 0){
		newData.age = oldData.age;
	}
	if (strlen(newData.name) <= 0){
		strcpy(newData.name, oldData.name);
	}
	if (newData.std_no < 0){
		newData.std_no = oldData.std_no;
	}
	if (newData.sex - 'a' == 0){
		newData.sex = oldData.sex;
	}
	printf("%d %s %d %c\n", newData.age, newData.name, newData.std_no, newData.sex);

	return InsertTable(newData, id, fd);

}

// 查找，id为文件的索引， fd为文件描述符
extern Data Select(int id, int fd){
	off_t endPos;
	Data record;
	int *p = NULL;
	char *q = NULL;
	int temp = 0;
	char str[NAME_LEN];

	q = str;
	p = &temp;

	endPos = get_offset(id, fd, READ);
//	printf("endPos %ld\n", endPos);

	if (endPos == -1){
		printf("get offset error.\n");
	}

	if (pread(fd, p, 4, endPos) == -1){
		printf("read id error.\n");
		exit(0);
	}
	endPos = endPos + 4;
	record.age = *p;

	if (pread(fd, q, NAME_LEN, endPos) == -1){
		printf("read name error.\n");
		exit(0);
	}
	endPos = endPos + NAME_LEN;
	strcpy(record.name, q);
	record.name[NAME_LEN - 1] = '\0';

	if (pread(fd, p, 4, endPos) == -1){
		printf("read std_no error.\n");
		exit(0);
	}
	endPos = endPos + 4;
	record.std_no = *p;

	if (pread(fd, q, 1, endPos) == -1){
		printf("read sex error.\n");
		exit(0);
	}
	record.sex = *q;

	return record;
}



// 插入表格， fd 为文件描述符
extern int InsertTable(Data record, int id, int fd){
	off_t endPos; // 偏移量
	int *p = NULL; // 指向int类型的指针;
	char *q = NULL; // 指向字符串类型的指针

        int  age = record.age;
	char name[NAME_LEN];
	int std_no = record.std_no;
	char sex = record.sex;

	strcpy(name, record.name);
	name[NAME_LEN-1] = '\0';

	endPos = get_offset(id, fd, WRITE);
	printf("endPos %ld\n", endPos);
	
	if(endPos == -1){
		printf("Find offset error!\n");
		return ERROR;
	}
	
        p = &age;
	if (pwrite(fd, p, 4, endPos) != 4){
		return ERROR;
	}
	endPos = endPos + 4;
	q = name;
	if (pwrite(fd, q, NAME_LEN, endPos) != NAME_LEN){
		return ERROR;
	}
	endPos = endPos + NAME_LEN;
	p = &std_no;
	if (pwrite(fd, p, 4, endPos) != 4){
		return ERROR;
	}
	endPos = endPos + 4;
	q = &sex;
	if (pwrite(fd, q, 1, endPos) != 1){
		return ERROR;
	}
	return SUCCESS;

}
