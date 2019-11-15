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

#define PORT (8040)
#define MAX_STRING_LEN (1024)

int main()
{
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
    char buf[1024] = "insert into student values(43, \"Frank\", 201507011, 'b');";
    send(sockfd, buf, strlen(buf),0);
    // 读取
    while(read(sockfd, buf, 1024))
    {
        printf("recv msg: %s\n", buf);
        memset(buf, 0, 1024);
    }
    return 0;
}
