#define TABLE_NAME  ("student")
#define INDEX_NAME ("index")

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#define OPEN_MODE (O_RDWR | O_CREAT | O_TRUNC)
#define MAX_STRING_LEN (100)
#define MAX_THREAD_LEN (100)

#define INSERT (1)
#define DELETE (2)
#define UPDATE (3)
#define SELECT (4)
#define CLEAN (5)
#define EXIT (6)
#define DEFAULT (7)

#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<time.h>
#include<unistd.h>
#include<fcntl.h>
#include<sys/stat.h>
#include<errno.h>
#include<pthread.h>

#include "BPTree.h"
#include "db.h"

int fd;
char str[MAX_STRING_LEN];
BPTree T = NULL;
// 操作提醒
void PrintMenu(){
	printf("			----- B+ Tree Database -----		\n");
	printf("I implement a database base on B+ tree, which can run well under high concurrency as well.\n");
	printf("The table of shape (int age; char *name; int std_no; char sex), details can be found in db.h.\n");
	printf("You can use the database as fllows:\n");
	printf("    For Insert: insert into student values(23, \"jamson\", 201507064, 'b');\n");
	printf("    For Detele: delete * from student where id = 10;  (id denotes the index in B+ tree).\n");
	printf("    For Update: updata student set age=43, name=\"Marry\" where id = 24;\n");
	printf("    For Select: select * from student where id = 10;\n");
	printf("clean: for clean the screen. \n"); 
	printf("exit: exit the database!\n");
	printf("   @Copyright jamson wan ,UESTC; email: 1940318814@qq.com, 2019.11.3\n");
}

// 解析命令
int GetCommand(char *str){
	int i = 0, j = 0;
	char cmd[MAX_STRING_LEN];

	while(str[i] != '\0'){
		while (str[i] == ' ' && str[i] != '\0')
			i++;
		while (str[i] != ' ' && str[i] != '\0'){
			cmd[j++] = str[i++];
		}
		break;
	}
	cmd[j--] = '\0';

	if (strncmp(cmd, "insert", j) == 0)
		return INSERT;
	if (strncmp(cmd, "delete", j) == 0)
		return DELETE;
        if (strncmp(cmd, "update", j) == 0)
		return UPDATE;
        if (strncmp(cmd, "select", j) == 0)
		return SELECT;
	if (strncmp(cmd, "clean", j) == 0)
		return CLEAN;
	if (strncmp(cmd, "exit", j) == 0)
		return EXIT;
	return DEFAULT;
}

// 词法分解
int ParseInput(char *str, char res[][MAX_STRING_LEN]){
	int j = 0, k = 0;
	int i = 0;
	while (str[k] != '\0'){
		while (str[k] == ' ' && str[k] != '\0') k++;
		while (str[k] != ' ' && str[k] != '\0'){
			res[i][j] = str[k];
			j++;
			k++;
		}
		res[i][j] = '\0';
		i++;
		j = 0;
	}
	return i;
}

int ParseInsert(char cmd[][MAX_STRING_LEN], int data_len, Data *p){
	int i = 1, j=0, k=0;
	int age = -1, std_no = -1;
	char name[MAX_STRING_LEN], sex;
	char temp[MAX_STRING_LEN];
	

	if (data_len != 7){
		printf("Syntax error!\n");
		return 0;
	}

	if (strncmp(cmd[i++], "into", 4) != 0){
		printf("Use into please!\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, 7) != 0){
		printf("Use %s please!\n", TABLE_NAME);
		return 0;
	}
	if (strncmp(cmd[i], "values(", 7) != 0){
		printf("Use values( please!\n");
		return 0;
	}
	if (cmd[data_len - 1][3] != ')' && cmd[data_len - 1][4] != ';'){
		printf("Use ); please!\n");
		return 0;
	}
	else{
		k = 7;
		while (cmd[i][k] != ','){
			temp[j++] = cmd[i][k++];
		}
		age = atoi(temp);
		i++;
		k = strlen(cmd[i]);
		if (k > 9){
			printf("The lenght of name is too long!\n");
			return 0;
		}
		k = 1; // skip '"'
		j = 0;
		while (cmd[i][k] != '"'){
			name[j++] = cmd[i][k++];
		}
		name[j] = '\0';

		i++;
		k = 0;
		j = 0;
		if ((strlen(cmd[i]) < 7) && (strlen(cmd[i]) > 14)){
			printf("The length of std_no should be 7-13 !\n");
			return 0;
		}
		while (cmd[i][k] != ','){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
		std_no = atoi(temp);
		
		i++;
		sex = cmd[i][1];
		if (sex != 'g' && sex != 'b'){
			printf("The sex should be 'b' or 'g'.\n");
			return 0;
		}
	}
	p->age = age;
	strcpy(p->name, name);
	p->std_no = std_no;
	p->sex = sex;

	return 1;
}

void * HandleInsert(void *arg){
	Map map = *(Map *)arg;
	int i = 0, temp = 0;
	Data newData;
	char cmd[MAX_STRING_LEN][MAX_STRING_LEN];

	i = ParseInput(str, cmd);
	temp = ParseInsert(cmd, i, &newData);

	if (temp == 1){
		printf("success\n");
		printf("age: %d, name: %s, std_no: %d, sex: %c\n", newData.age, newData.name, newData.std_no, newData.sex);
		T = Insert(T, map.key, map.offset);
		i = InsertTable(newData, map.offset, fd);
		if (i == SUCCESS){
			printf(" (1) row effect.\n");
			return (void *)1;
		}
	}
	return (void *)0;

}


int main(int argc, const char* argv[]){
	int i, temp, cmd;
	int exit_flag = 0;
	pthread_t thread_id[MAX_THREAD_LEN];
	int thread_no = 0;
	Map map;

	T = Initialize();
	map.key = 0;
	map.offset = 0;

	if (access(INDEX_NAME, F_OK) == 0){
		T = CreatBPTree(T);
		if (T == NULL){
			printf("Creat BPTree error!\n");
			return 0;
		}
		map = SearchLast(T);
	}
	
	if ((fd = open(TABLE_NAME, OPEN_MODE, FILE_MODE)) == -1){
		printf("Create or Open data file error!\n");
		return 0;
	}

	printf("%d  %ld\n", map.key, map.offset);

        PrintMenu(); 
	while(1){
		cmd = -1;
		printf(">>");
		fgets(str, MAX_STRING_LEN, stdin);

		cmd = GetCommand(str);

		switch(cmd){
			case INSERT:

				map.key++;
				map.offset++;

				if (pthread_create(&thread_id[thread_no++], NULL, HandleInsert, &map) != 0){
					printf("create thread error!\n");
					return 0;
				}
				break;

			case EXIT:
				exit_flag = 1;
				break;
			case CLEAN:
				system("clear");
				PrintMenu();
				break;
			case DEFAULT:
				printf("Syntax error\n");
				break;
			default:
				printf("I will imeplement this function in future.\n");
				break;
		}
		
		if(exit_flag)  break;
	}
	i = 0;
	for (;i < thread_no; i++){
		pthread_join(thread_id[i], NULL);
	}

	temp = SaveIndex(T);
	if (temp == SUCCESS){
		printf("Save successed!\n");
	}else
		printf("Save error\n");

	close(fd);

	Destroy(T);
	return 0;
}
