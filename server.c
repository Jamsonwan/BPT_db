#define TABLE_NAME  ("student")
#define INDEX_NAME ("index")
#define DELETE_INDEX ("delete_index")

#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)
#define OPEN_MODE (O_RDWR | O_CREAT | O_TRUNC)
#define MAX_STRING_LEN (1024)
#define MAX_THREAD_LEN (100)
#define MAX_INDEX (100000)

#define INSERT (1)
#define DELETE (2)
#define UPDATE (3)
#define SELECT (4)
#define CLEAN (5)
#define EXIT (6)
#define DEFAULT (7)
#define PORT (8040)

#include<stdlib.h>
#include<stdio.h>
#include<string.h>
#include<time.h>
#include<unistd.h>
#include<fcntl.h>
#include<sys/stat.h>
#include<errno.h>
#include<pthread.h>
#include<sys/socket.h>
#include<sys/types.h>
#include<sys/select.h>
#include<arpa/inet.h>
#include<netinet/in.h>

#include "BPTree.h"
#include "db.h"


int fd;
char str[MAX_STRING_LEN];
Map map;
BPTree T = NULL;

char Syntax_error[] = "Syntax error!";
char System_error[] = "System error!";
char Success_msg[] =  "<1> row effect!";

pthread_mutex_t bmutex=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t fmutex=PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t index_mutex=PTHREAD_MUTEX_INITIALIZER;

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
//		printf("Syntax error!\n");
		return 0;
	}

	if (strncmp(cmd[i++], "into", 4) != 0){
//		printf("Use into please!\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, 7) != 0){
//		printf("Use %s please!\n", TABLE_NAME);
		return 0;
	}
	if (strncmp(cmd[i], "values(", 7) != 0){
//		printf("Use values( please!\n");
		return 0;
	}
	if (cmd[data_len - 1][3] != ')' && cmd[data_len - 1][4] != ';'){
//		printf("Use ); please!\n");
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
//			printf("The lenght of name is too long!\n");
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
//			printf("The length of std_no should be 7-13 !\n");
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
//			printf("The sex should be 'b' or 'g'.\n");
			return 0;
		}
	}
	p->age = age;
	strcpy(p->name, name);
	p->std_no = std_no;
	p->sex = sex;

	return 1;
}

void  HandleInsert(char *str, Map t_map, int connect_fd){
	int i = 0, temp = 0;
	Data newData;
	char cmd[MAX_STRING_LEN][MAX_STRING_LEN];

	i = ParseInput(str, cmd);
	temp = ParseInsert(cmd, i, &newData);

	if (temp == 1){
		T = Insert(T, t_map.key, t_map.offset);
		i = SaveIndex(T);
		if (i == 0){
			temp =send(connect_fd,  System_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
			return;
		}
		i = 0;
		i = InsertTable(newData, t_map.offset, fd);
		if (i == SUCCESS){
//			printf(" <1> row effect.\n>>");
			temp =send(connect_fd,  Success_msg, 20, 0);
			if (temp < 0)
				printf("send error!\n");
		}else{
			temp =send(connect_fd,  System_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
		}
	}else{
		temp =send(connect_fd,  Syntax_error, 20, 0);
		if (temp < 0)
			printf("send error!\n");
	}
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
void * HandleSelect(char *str, int connect_fd){
	Map t_map;
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
			temp =send(connect_fd,  Syntax_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
		}
		t_map.offset = p->pt->value[p->i];
		pthread_mutex_lock(&fmutex);
		data = Select(t_map.offset, fd);
		pthread_mutex_unlock(&fmutex);
		printf("name	std_no		age	sex\n");
		printf("%s	%d	%d	%c\n", data.name, data.std_no, data.age, data.sex);
		printf(">>");
		return (void *)1;
	}else{
		temp =send(connect_fd,  Syntax_error, 20, 0);
		if (temp < 0)
			printf("send error!\n");
	}
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
//		printf("Syntax error!\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, strlen(TABLE_NAME)) != 0){
//		printf("Syntax error!\n");
		return 0;
	}
	if (strncmp(cmd[i++], "set", 3) != 0){
//		printf("Syntax error!\n");
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
//		printf("Syntax error! Loss 'where'!\n");
		return 0;
	}

	if (contain_char(cmd[i], '=')){
		if (cmd[i][0] != 'i' && cmd[i][1] != 'd'){
//			printf("Use 'id=' please! Other function will implement in future!\n");
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
//			printf("Syntax error! Loss ';'\n");
			return 0;
		}
	}else{
	       	if(strncmp(cmd[i++], "id", 2) != 0){
//			printf("Use 'id' please! Other function will implement in future!\n");
			return 0;
		}

		if (cmd[i][0] != '='){
//			printf("Syntax error!\n");
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
//			printf("Syntax error! Loss ';'\n");
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
void  HandleUpdate(char *str,  int connect_fd){
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
//			printf("No such id\n>>");	
			temp = send(connect_fd,  Syntax_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
			return;
		}
		pthread_mutex_lock(&fmutex);
		i = Update(p->pt->value[p->i], fd, newData);
		pthread_mutex_unlock(&fmutex);
		if (i == SUCCESS){
//			printf("<1> row effect.\n>>");
			temp =send(connect_fd,  Success_msg, 20, 0);
			if (temp < 0)
				printf("send error!\n");
		}else{
			temp =send(connect_fd,  System_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
		}
	}else{
		temp =send(connect_fd,  Syntax_error, 20, 0);
		if (temp < 0)
			printf("send error!\n");
	}
}

// 解析删除
int ParseDelete(char cmd[][MAX_STRING_LEN], int data_len, KeyType *key){
	int i, j, k;
	char temp[MAX_STRING_LEN];

	if (data_len < 6){
//		printf("Syntax error!\n");
		return 0;
	}
	i = 1;

	if (cmd[i++][0] != '*'){
//		printf("Use 'delete *' please! Other function will implement in future|\n");
		return 0;
	}
	if (strncmp(cmd[i++], "from", 4) != 0){
//		printf("Syntax error! Loss 'from';\n");
		return 0;
	}
	if (strncmp(cmd[i++], TABLE_NAME, 7) != 0){
//		printf("Use %s please! Other function will implement in future!\n", TABLE_NAME);
		return 0;
	}
	if (strncmp(cmd[i++], "where", 5) != 0){
//		printf("Syntax error! Loss 'where';\n");
		return 0;
	}
	if (contain_char(cmd[i], '=')){
		if (cmd[i][0] != 'i' && cmd[i][1] != 'd'){
//			printf("Use 'where id= ' please, other function will implement in future.\n");
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
//			printf("Syntax error, loss ';'.\n");
			return 0;
		}
	}else{
		if (strncmp(cmd[i++], "id", 2) != 0){
//			printf("Use 'id = ' please! Other function will implement in future.\n");
			return 0;
		}
		if (cmd[i++][0] != '='){
//			printf("Syntax error! Loss '='.;\n");
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
//			printf("Syntax error! Loss ';'.\n");
			return 0;
		}
	}
	return 1;
}


// 处理删除
void  HandleDelete(char *str, int connect_fd){
	int data_len, temp;
	KeyType key;
	Result *p, *q;
	char cmd[MAX_STRING_LEN][MAX_STRING_LEN];

	data_len = ParseInput(str, cmd);
	temp = ParseDelete(cmd, data_len, &key);
	if (temp == 1){
		pthread_mutex_lock(&bmutex);
		p = SearchBPTree(T, key);
		if (p->tag == 0){
//			printf("No Such id!\n>>");
			pthread_mutex_unlock(&bmutex);
			temp =send(connect_fd,  Syntax_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
			return;
		}
		T = Remove(T, key);
		q = SearchBPTree(T, key);		
		if (q->tag == 0){
			SaveIndex(T);
			pthread_mutex_unlock(&bmutex);
			temp =send(connect_fd,  Success_msg, 20, 0);
			if (temp < 0)
				printf("send error!\n");
		}else{
			pthread_mutex_unlock(&bmutex);
			temp =send(connect_fd,  Syntax_error, 20, 0);
			if (temp < 0)
				printf("send error!\n");
			return;
		}

		
	}else{
		temp =send(connect_fd,  Syntax_error, 20, 0);
		if (temp < 0)
			printf("send error!\n");
	}			
}

void * pthread_work(void * arg){
	int connect_fd = * (int *)arg;
	int numbytes = 0, cmd = -1;
	int flag = 0;
	char buff[MAX_STRING_LEN];

        while((numbytes = recv(connect_fd, buff, MAX_STRING_LEN, 0)) > 0){
		buff[numbytes] = '\0';	
		cmd = GetCommand(buff);

		switch(cmd){
			case INSERT:
				pthread_mutex_lock(&index_mutex);
				map.key++;
				map.offset++;
				pthread_mutex_unlock(&index_mutex);
				HandleInsert(buff, map, connect_fd);
				break;
//			case SELECT:
//				HandleSelect(buff, connect_fd);
//				break;
			case DELETE:
				HandleDelete(buff, connect_fd);
				break;
			case UPDATE:
				HandleUpdate(buff, connect_fd);
				break;
			default:
				flag = 1;
				printf("I will imeplement this function in future.\n>>");
				break;
		}
		if (flag == 1) break;		
	}
	close(connect_fd);	
}

int main(int argc, const char* argv[]){
	int i, temp;
	pthread_t thread_id;

	char buff[MAX_STRING_LEN];
	int numbytes=0;
	int connect_fd=0;
	int sockfd = socket(AF_INET, SOCK_STREAM, 0);
	struct sockaddr_in *server_socket = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
	struct sockaddr_in *client_socket = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
	socklen_t client_address_len;
	
	
	// initialize B+ tree
	T = Initialize();
	map.key = 0;
	map.offset = -1;
	
	// read B+ tree from file
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
	
    	// open data file
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

	memset(server_socket, 0, sizeof(struct sockaddr_in));
	memset(client_socket, 0, sizeof(struct sockaddr_in));

	server_socket->sin_addr.s_addr = htonl(INADDR_ANY);
	server_socket->sin_family = AF_INET;
	server_socket->sin_port = htons(PORT);

	bind(sockfd, (struct sockaddr *)server_socket, sizeof(struct sockaddr));
	printf("Listening.....\n");
	listen(sockfd, 20);
	
	while(1){
		printf("Start accept new client incoming...\n");
		connect_fd = accept(sockfd, (struct sockaddr *)client_socket, &client_address_len);
		printf("Accept new client[%s: %d] successfully\n", inet_ntoa(client_socket->sin_addr), ntohs(client_socket->sin_port));
		if (pthread_create(&thread_id, NULL, pthread_work, &connect_fd) != 0){
			printf("create thread error!\n");
			return 0;
		}	
	}
	pthread_join(thread_id, NULL);

	close(sockfd);
	close(fd);

	Destroy(T);
	return 0;

}
