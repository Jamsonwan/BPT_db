#define TABLE_NAME  ("student")
#define INDEX_NAME ("index")
#define DELETE_INDEX ("delete_index")

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#define OPEN_MODE (O_RDWR | O_CREAT | O_TRUNC)
#define MAX_STRING_LEN (100)
#define MAX_THREAD_LEN (100)
#define MAX_INDEX (100000)

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

pthread_mutex_t bmutex=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t fmutex=PTHREAD_MUTEX_INITIALIZER;
//Map deleteIndex[MAX_INDEX];

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
		T = Insert(T, map.key, map.offset);
		i = SaveIndex(T);
		if (i == 0){
			printf("Save Index Error.\n>>");
			return (void *)0;
		}
		i = 0;
		i = InsertTable(newData, map.offset, fd);
		if (i == SUCCESS){
			printf(" (1) row effect.\n>>");
			return (void *)1;
		}
	}
	printf(">>");
	return (void *)0;

}

// 解析iselect
int ParseSelect(char cmd[][MAX_STRING_LEN], int data_len, KeyType *key){
	int i = 1, j=0, k = 0;
	char temp[MAX_STRING_LEN];

	if (data_len != 8 && data_len != 6){
		printf("Syntax error!\n");
		return 0;
	}

	if (cmd[i++][0] != '*'){
		printf("Use select * from please! Other function will implement in future!\n");
		return 0;
	}
	if (strncmp(cmd[i++], "from", 4) != 0){
		printf("Use from please!\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, 7) != 0){
		printf("Use %s please! Other function will implement in future!\n", TABLE_NAME);
		return 0;
	}
	if (strncmp(cmd[i++], "where", 5) != 0){
		printf("Use where please!\n");
		return 0;
	}
	if (data_len == 6){
		temp[0] = cmd[i][0];
		temp[1] = cmd[i][1];
		temp[2] = cmd[i][2];
		j = 3;
		k = 0;
		if (strncmp(temp, "id=", 3) != 0){
			printf("Use id= please! Other function will implement in future!\n");
			return 0;
		};

		while (cmd[i][j] != ';' && cmd[i][j] != '\0'){
			temp[k++] = cmd[i][j++];
		}
		temp[k] = '\0';
		*key = atoi(temp);

		if (cmd[i][j] != ';'){
			printf("Syntax error! Loss ';'\n");
			return 0;
		}
		
	}else{
		temp[0] = cmd[i][0];
		temp[1] = cmd[i][1];
		if (strncmp(temp, "id", 2) != 0){
			printf("Use id = please! Other function will implement in future!\n");
			return 0;
		}
		i++;
		if (cmd[i][0] != '='){
			printf("Use '=' please!\n");
			return 0;
		}
		i++;
		k=0;
		j = 0;
		while (cmd[i][k] != ';' && cmd[i][k] != '\0'){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
		*key = atoi(temp);

		if (cmd[i][j] != ';'){
			printf("Syntax error! Loss ';'!\n");
			return 0;
		}
	}

	return 1;
}



// 执行查询操作， 暂时只实现只能查询一条语句
void * HandleSelect(void *arg){
	Map map;
	int i = 0, temp = 0;
	KeyType key;

	Result *p;

	Data data;

	char cmd[MAX_STRING_LEN][MAX_STRING_LEN];

	i = ParseInput(str, cmd);
	temp = ParseSelect(cmd, i, &key);

	if (temp == 1){
		pthread_mutex_lock(&bmutex);
		p = SearchBPTree(T, key);
		pthread_mutex_unlock(&bmutex);
		if (p->tag == 0){
			printf(" No Such id!\n>>");
			return (void *)0;
		}
		map.offset = p->pt->value[p->i];
		pthread_mutex_lock(&fmutex);
		data = Select(map.offset, fd);
		pthread_mutex_unlock(&fmutex);
		printf("name	std_no		age	sex\n");
		printf("%s	%d	%d	%c\n", data.name, data.std_no, data.age, data.sex);
		printf(">>");
		return (void *)1;
	}
	printf(">>");	
	return (void *)0;
}

// 判断一个字符串是否包含一个字符ch
int contain_char(char *p, char ch){
	while (*p != '\0'){
		if (*p == ch){
			return 1;
		}
		p++;
	}
	return 0;
}	

// 解析Update
int ParseUpdate(char cmd[][MAX_STRING_LEN], int data_len, Data *p, int *id){
	int i = 1, j = 0, k = 0;
	int age = -1, std_no = -1;
	char sex = 'a';
	char name[MAX_STRING_LEN];
	int flag = 0;
	char temp[MAX_STRING_LEN];

	name[0] = '\0';

	if (data_len < 6 || data_len > 11){
		printf("Syntax error!\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, strlen(TABLE_NAME)) != 0){
		printf("Syntax error!\n");
		return 0;
	}
	if (strncmp(cmd[i++], "set", 3) != 0){
		printf("Syntax error!\n");
		return 0;
	}
	while (i < data_len){
		flag = contain_char(cmd[i], '=');
		if (!flag) break;

		j = 0;
		k = 0;
		while (cmd[i][k] != '='){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
		k++; // skip '='

		if (strncmp(temp, "age", 3) == 0)  flag = 1;
		if (strncmp(temp, "name", 4) == 0)  flag = 2;
		if (strncmp(temp, "sex", 3) == 0)  flag = 3;
		if (strncmp(temp, "std_no", 6) == 0) flag = 4;
	       
	       	j = 0;
		switch (flag){
			case 1:
				while (cmd[i][k] != ',' && cmd[i][k] != '\0'){
					temp[j++] = cmd[i][k++];
				}
				temp[j] = '\0';
				age = atoi(temp);
				break;
			case 2:
				k++; // skip '"'
				while (cmd[i][k] != '"'){
					temp[j++] = cmd[i][k++];
				}
				temp[j] = '\0';
				strncpy(name, temp, j);
				break;
			case 4:
				while (cmd[i][k] != ',' && cmd[i][k] != '\0'){
					temp[j++] = cmd[i][k++];
				}
				temp[j] = '\0';
				std_no = atoi(temp);
				break;
			case 3:
				k++; //skip '\'
				sex = cmd[i][k];
				break;
		}
		flag = 0;
		i++;
	}

	if (strncmp(cmd[i++], "where", 5) != 0){
		printf("Syntax error! Loss 'where'!\n");
		return 0;
	}

	if (contain_char(cmd[i], '=')){
		if (cmd[i][0] != 'i' && cmd[i][1] != 'd'){
			printf("Use 'id=' please! Other function will implement in future!\n");
			return 0;
		}
		k = 3; // skip '='
		j = 0;
		while (cmd[i][k] != ';' && cmd[i][k] != '\0'){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
	//	printf("id: %s\n",temp);
		*id = atoi(temp);

		if (cmd[i][k] != ';'){
			printf("Syntax error! Loss ';'\n");
			return 0;
		}
	}else{
	       	if(strncmp(cmd[i++], "id", 2) != 0){
			printf("Use 'id' please! Other function will implement in future!\n");
			return 0;
		}

		if (cmd[i][0] != '='){
			printf("Syntax error!\n");
		 	return 0;
		}
		i++;
		j = 0;
		k = 0;
		while (cmd[i][k] != ';' && cmd[i][k] != '\0'){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
		*id = atoi(temp);

		if (cmd[i][k] != ';'){
			printf("Syntax error! Loss ';'\n");
			return 0;
		}	        
	}
	p->age = age;
	strcpy(p->name, name);
	p->std_no = std_no;
	p->sex = sex;

	return 1;

}

// 进行更新
void * HandleUpdate(void *arg){
	Data newData;
	KeyType key;
	Result *p;

	char cmd[MAX_STRING_LEN][MAX_STRING_LEN];
	int i = 0, temp = 0;

	i = ParseInput(str, cmd);
	temp = ParseUpdate(cmd, i, &newData, &key);
	if (temp == 1){
		pthread_mutex_lock(&bmutex);
		p = SearchBPTree(T, key);
		pthread_mutex_unlock(&bmutex);
		if (p->tag == 0){
			printf("No such id\n>>");
			return (void *)0;
		}
		pthread_mutex_lock(&fmutex);
		i = Update(p->pt->value[p->i], fd, newData);
		pthread_mutex_unlock(&fmutex);
		if (i == SUCCESS){
			printf(" (1) row effect.\n>>");
			return (void *)1;
		}
	}
	printf(">>");
	return (void *)0;
}

// 解析删除
int ParseDelete(char cmd[][MAX_STRING_LEN], int data_len, KeyType *key){
	int i, j, k;
	char temp[MAX_STRING_LEN];

	if (data_len < 6){
		printf("Syntax error!\n");
		return 0;
	}
	i = 1;

	if (cmd[i++][0] != '*'){
		printf("Use 'delete *' please! Other function will implement in future|\n");
		return 0;
	}
	if (strncmp(cmd[i++], "from", 4) != 0){
		printf("Syntax error! Loss 'from';\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, 7) != 0){
		printf("Use %s please! Other function will implement in future!\n", TABLE_NAME);
		return 0;
	}
	if (strncmp(cmd[i++], "where", 5) != 0){
		printf("Syntax error! Loss 'where';\n");
		return 0;
	}
	if (contain_char(cmd[i], '=')){
		if (cmd[i][0] != 'i' && cmd[i][1] != 'd'){
			printf("Use 'where id= ' please, other function will implement in future.\n");
			return 0;
		}
		k = 3; // skip '='
		j = 0;
		while (cmd[i][k] != ';' && cmd[i][k] != '\0'){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
		*key = atoi(temp);
		if (cmd[i][k] != ';'){
			printf("Syntax error, loss ';'.\n");
			return 0;
		}
	}else{
		if (strncmp(cmd[i++], "id", 2) != 0){
			printf("Use 'id = ' please! Other function will implement in future.\n");
			return 0;
		}
		if (cmd[i++][0] != '='){
			printf("Syntax error! Loss '='.;\n");
			return 0;
		}
		k = 0;
		j = 0;
		while (cmd[i][k] != ';' && cmd[i][k] != '\0'){
			temp[j++] = cmd[i][k++];
		}
		temp[j] = '\0';
		*key = atoi(temp);
		if (cmd[i][k] != ';'){
			printf("Syntax error! Loss ';'.\n");
			return 0;
		}
	}
	return 1;
}


// 处理删除
void * HandleDelete(void *arg){
	int data_len, temp;
	KeyType key;
	Result *p, *q;
//	Map map;
	char cmd[MAX_STRING_LEN][MAX_STRING_LEN];

	data_len = ParseInput(str, cmd);
	temp = ParseDelete(cmd, data_len, &key);
	if (temp == 1){
		pthread_mutex_lock(&bmutex);
		p = SearchBPTree(T, key);
		if (p->tag == 0){
			printf("No Such id!\n>>");
			pthread_mutex_unlock(&bmutex);
			return (void *) 0;
		}
		T = Remove(T, key);
		q = SearchBPTree(T, key);

		pthread_mutex_unlock(&bmutex);
		if (q->tag == 0){
//			delete_index[index] = map;
	//		SaveDeleteIndex(index,map);
			SaveIndex(T);
			printf(" <1> row effect.\n>>");
			return (void *)1;
		}
		printf("System Error!\n");
	}

	printf(">>");
	return (void *)0;
}

int main(int argc, const char* argv[]){
	int i, temp, cmd;
	int delete_fd;
	int exit_flag = 0;
	pthread_t thread_id[MAX_THREAD_LEN];
	int thread_no = 0;
//	int index_no = 0;
//	unsigned long delete_len;
	Map map;
//	KeyType  *p;
//	Record *q;

	T = Initialize();
	map.key = 0;
	map.offset = -1;
/*
	p = &delete_index[index_no].key;
	q = &delete_index[index_no].offset;

	if (access(DELETE_INDEX, F_OK) == 0){
                delete_len = get_file_size(DELETE_INDEX);

		i = 0;
		if ((delete_fd = open(DELETE_INDEX, O_RDWR)) == -1){
		       printf("Open delete_index failed!\n");
	       	       return 0;
		}
		while (i < delete_len){
			if ((pread(delete_fd, p, 4, i)) == -1){
				close(delete_fd);
				return 0;
			}
			if ((pread(delete_fd, q, 4, i+4)) == -1){
				close(delete_fd);
				return 0;
			}
			i += 8;
			index_no++;

			p = &delete_index[index_no].key;
			q = &delete_index[index_no].offset;
		}
		close(delete_fd);

	}
	*/
	if (access(INDEX_NAME, F_OK) == 0){
		T = CreatBPTree(T);
		if (T == NULL){
			printf("Creat BPTree error!\n");
			return 0;
		}
		temp = get_file_size(INDEX_NAME);
		if (temp > 0){
			map = SearchLast(T);
		}
	}
	
	if (access(TABLE_NAME, F_OK) == 0){
		if ((fd = open(TABLE_NAME, O_RDWR)) == -1){
			printf("Open data file error!\n");
			return 0;
		}
		
	}else{
		if ((fd = open(TABLE_NAME, OPEN_MODE, FILE_MODE)) == -1){
			printf("Create data file error!\n");
			return 0;
		}
	}

        PrintMenu(); 
	printf(">>");
	while(1){
		cmd = -1;
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
			case SELECT:
				if (pthread_create(&thread_id[thread_no++], NULL, HandleSelect, NULL) != 0){
					printf("create thread error!\n");
					return 0;
				}
				break;
			case DELETE:
				if (pthread_create(&thread_id[thread_no++], NULL, HandleDelete, NULL) != 0){
					printf("create thread error!\n");
					return 0;
				}
				break;
			case UPDATE:
				if (pthread_create(&thread_id[thread_no++], NULL, HandleUpdate, NULL) != 0){
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
				printf("Syntax error\n>>");
				break;
			default:
				printf("I will imeplement this function in future.\n>>");
				break;
		}
		
		i = 0;
		for (;i < thread_no; i++){
			pthread_join(thread_id[i], NULL);
		}
		thread_no = 0;
		if(exit_flag)  break;
	}

	temp = SaveIndex(T);
	if (temp == ERROR){
		printf("Save index error!\n");
	}

	close(fd);

	Destroy(T);

	return 0;

}
