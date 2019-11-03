#define FILE_NAME ("data")
#define INDEX_NAME ("index")
#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#define OPEN_MODE (O_RDWR | O_CREAT | O_TRUNC)
#define MAX_STRING_LEN (100)

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
#include "BPTree.h"
#include "db.h"

void PrintMenu(){
	printf("			----- B+ Tree Database -----		\n");
	printf("I implement a database base on B+ tree, which can run well under high concurrency as well.\n");
	printf("The table of shape (int age; char *name; int std_no; char sex), details can be found in db.h.\n");
	printf("You can use the database as fllows:\n");
	printf("    For Insert: insert into student (23, \"jamson\", 201507064, 'b');\n");
	printf("    For Detele: delete * from student where id = 10;  (id denotes the index in B+ tree).\n");
	printf("    For Update: updata student set age=43, name=\"Marry\" where id = 24;\n");
	printf("    For Select: select * from student where id = 10;\n");

	printf("   @Copyright jamson wan ,UESTC; email: 1940318814@qq.com, 2019.11.3\n");
	printf("clean: for clean the screen. \n"); 
	printf("exit: exit the database!\n");
	
}

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




int main(int argc, const char* argv[]){
	int i, temp, cmd;
	int exit_flag = 0;
        BPTree T;
	Result *res;
	char str[MAX_STRING_LEN];

	T = Initialize();

	if (access(INDEX_NAME, F_OK) == 0){
		T = CreatBPTree(T);
		if (T == NULL){
			printf("Creat BPTree error!\n");
			return 0;
		}
	}

        PrintMenu(); 
	while(1){
		cmd = -1;
		printf(">>");
		fgets(str, MAX_STRING_LEN, stdin);

		cmd = GetCommand(str);

		switch(cmd){
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


	temp = SaveIndex(T);
	if (temp == SUCCESS){
		printf("Save successed!\n");
	}else
		printf("Save error\n");


	Destroy(T);
	return 0;
}
