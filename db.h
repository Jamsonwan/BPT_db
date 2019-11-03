#ifndef db_H
#define db_h

#define NAME_LEN (11)
#define RECORD_LEN (NAME_LEN+9)
#define READ (1)
#define WRITE (2)
#define ERROR (-1)
#define SUCCESS (1)

#include<stdio.h>

typedef struct Table{
	int age; // age of one stu
	char name[NAME_LEN]; // the name of student
	int std_no;  // student ID 
	char sex; // 性别 ‘b': boy , 'g': girl
}Table, Data,*tb;

extern int InsertTable(Data record, int id, int fd);

extern int Delete(Data record, int fd);

extern int Update(int id, int fd, Data newData);

extern Table Select(int id, int fd);

#endif
