#include <stdio.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PORT (8040)
#define MAX_STRING_LEN (1024)

void itoa(int n, char s[]){
	int i,k, j, sign;
	char temp[MAX_STRING_LEN];

	sign = n;
	if(sign < 0)  n = -n;

	i = 0;
	do{
		temp[i++] = n % 10 + '0';
	}while((n /= 10) > 0);
	
	if(sign < 0){
		temp[i++] = '-';
	}

	for (j=i-1, k=0; j >= 0; j--, k++)
		s[k] = temp[j];
	s[k] = '\0';
}

void create_cmd(char *str){
	char insert_cmd[MAX_STRING_LEN] = {"insert into student values(43, \"liyiyi\", 201452215, 'b');"};
	char delete_cmd[MAX_STRING_LEN] = {"delete * from where id="};
	char update_cmd[MAX_STRING_LEN] = {"update student set age=22, name=\"Marry\" where id = "};

	int id, t_len;
	int ret;
	char temp[MAX_STRING_LEN];
	
	ret = rand() % 3;

	if (ret == 0){
		strncpy(str, insert_cmd, strlen(insert_cmd));

	}else{
		id = rand() % 100000;
		itoa(id, temp);
		t_len = strlen(temp);
		temp[t_len] = ';';
		temp[t_len+1] = '\0';
		if (ret == 1){
			strncpy(str, delete_cmd, strlen(delete_cmd));
			strncat(str, temp, strlen(temp));
		}else{
			strncpy(str, update_cmd, strlen(update_cmd));
			strncat(str, temp, strlen(temp));
		}
	}
}

int main()
{
    char buff[MAX_STRING_LEN];
    int i = 0, ret;

    // 创建socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    // 准备地址结构体
    struct sockaddr_in *server_addr = (struct sockaddr_in *)malloc(sizeof (struct sockaddr_in));
    memset(server_addr, 0, sizeof (struct sockaddr_in));
    server_addr->sin_family = AF_INET;
    server_addr->sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &server_addr->sin_addr);
    // 连接
    connect(sockfd, (struct sockaddr *)server_addr, sizeof (struct sockaddr)); // sockaddr_in强制转换成sockaddr
    srand((unsigned int)time(0));
    for(i=0; i < 100; i++){
	    memset(buff, '\0', MAX_STRING_LEN);
	    create_cmd(buff);
	    printf("Send command: %s\n", buff);
            ret = send(sockfd, buff, strlen(buff),0);
	    if (ret == -1){
		    printf("send error!\n");
		    break;
	    }
	    ret = read(sockfd, buff, MAX_STRING_LEN);
	    if (ret == -1){
		    break;
	    }
	    printf("recv msg: %s\n", buff);
	    memset(buff, '\0', MAX_STRING_LEN);
	    sleep(10);
    }
    send(sockfd, NULL, 0, 0);
    close(sockfd);
    return 0;
}
