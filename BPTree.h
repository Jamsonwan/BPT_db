#ifndef BPTree_h
#define BPTree_h

#include<fcntl.h>
#include<unistd.h>
#include<stdio.h>
#include<stdlib.h>
#include<limits.h>
#include<sys/stat.h>

#define M (3) // the order of B+ Tree
#define LIMIT_M_2 (M % 2 ? (M+1) / 2 : M / 2)
#define True (1)
#define False (0)
#define INDEX_NAME ("index")
#define SUCCESS (1)
#define ERROR (-1)
#define OPEN_MODE (O_RDWR | O_CREAT | O_TRUNC)
#define FILE_MODE (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)

typedef int KeyType; // 关键字的数据类型
typedef off_t Record; // 索引所对应的值

typedef struct BPTNode{
	int isLeaf;
	int keynum;
	KeyType key[M + 1];
	struct BPTNode *ptr[M + 1];
	struct BPTNode *next;
	Record value[M + 1];
}BPTNode, *BPTree;

typedef struct Result{
	BPTNode *pt;
	int i;
	int tag;
}Result;

typedef struct Map{
	KeyType key;
	off_t offset;
}Map;

extern BPTree Initialize();

extern BPTree Insert(BPTree T, KeyType K, Record V);

extern BPTree Remove(BPTree T, KeyType K);

extern BPTree Destroy(BPTree T);

extern Result* SearchBPTree(BPTree T, KeyType K);

extern void Travel(BPTree T);

extern int SaveIndex(BPTree T);

extern BPTree CreatBPTree(BPTree T);

extern Map SearchLast(BPTree T);

extern void TravelData(BPTree T);

#endif
