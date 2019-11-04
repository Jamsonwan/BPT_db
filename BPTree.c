#include "BPTree.h"

static KeyType Unavailable = INT_MIN;

// 加载索引结点
extern int SaveIndex(BPTree T){
	int fd;
	int i = 0, j = 0;
	KeyType key;
	Record record;
	char *p;
        char *q;

	BPTree temp = T;

	if (T == NULL)
		return 0;

	if ((fd = open(INDEX_NAME, OPEN_MODE, FILE_MODE)) == -1){
		printf("Create or Open index file error!\n");
		return ERROR;
	}
	while (temp->ptr[0] != NULL)
		temp = temp->ptr[0];
	while (temp != NULL){
		i = 0;
		while (i < temp->keynum){
			key = temp->key[i];
			record = temp->value[i];
			p =(char *)&key;
			q =(char *)&record;
			if (pwrite(fd, p, 4, j) == -1){
				close(fd);
				return ERROR;
			}
			if (pwrite(fd, q, 4, j+4) == -1){
				close(fd);
				return ERROR;
			}
			i++;
			j += 8;
		}
		temp = temp->next;
	}
	close(fd);

	return SUCCESS;

}

//得到文件的长度
static unsigned long get_file_size(const char *filename){
	struct stat buf;
	if (stat(filename, &buf) < 0)
		return 0;
	else
		return buf.st_size;
}
// 从索引文件中读取数据，创建b+树
extern BPTree CreatBPTree(BPTree T){
	int fd;
	int i = 0, ret = -1;
	unsigned long file_len = -1;
	struct stat statbuff;
	KeyType temp = 0;
	KeyType *q;
	Record t_record = 0;
        Record *p;

	q = &temp;
	p = &t_record;

	file_len = get_file_size(INDEX_NAME);

	if ((fd = open(INDEX_NAME, O_RDONLY)) == -1){
		printf("Open index file failled!\n");
		return NULL;
	}
	
	while (i < file_len){
		if ((ret = pread(fd, q, sizeof(KeyType), i)) == -1){
			close(fd);
			return NULL;
		}
		if ((ret = pread(fd, p, sizeof(Record), i+4)) == -1){
			close(fd);
			return NULL;
		}
		T = Insert(T, *q, *p);
		i = i + sizeof(KeyType) + sizeof(Record);
	}
	close(fd);

	return T;
}

// 查找最后的键值对
extern Map SearchLast(BPTree T){
	Map ret;

	BPTree temp = T;
	while (temp->ptr[0] != NULL){
		temp = temp->ptr[temp->keynum-1];
	}

	ret.offset = temp->value[temp->keynum-1];
	ret.key = temp->key[temp->keynum-1];

	return ret;
}


/* 生成结点初始化 */
static BPTree MallocNewNode(){
	int i;
	BPTree newNode = malloc(sizeof(BPTNode));
	if(newNode == NULL)
		exit(EXIT_FAILURE);

	i = 0;
	while (i < M + 1){
		newNode->key[i] = Unavailable;
		newNode->value[i] = Unavailable;
		newNode->ptr[i] = NULL;
		i++;
	}
	newNode->isLeaf = False;
	newNode->next = NULL;
	newNode->keynum = 0;

	return newNode;
}

/**
 * 初始化 带头结点的B+ 树
 * */
extern BPTree Initialize(){
	BPTree T;
	if (M < (3)){
		printf("M最小等于3！");
		exit(EXIT_FAILURE);
	}
	//根结点
	T = MallocNewNode();
	return T;
}

/* 找到最左边的叶子结点 */
static BPTree FindMostLeft(BPTree T){
	BPTree temp;

	temp = T;
	while (temp != NULL && temp->ptr[0] != NULL){
		temp = temp->ptr[0];
	}
	return temp;
}

/* 找到最右边的叶子结点 */
static BPTree FindMostRight(BPTree T){
	BPTree temp = T;

	while(temp != NULL && temp->ptr[temp->keynum-1] != NULL){
		temp = temp->ptr[temp->keynum - 1];
	}
	return temp;
}
/* 寻找一个兄弟结点，其存储的关键字没有满， 否则返回NULL */
static BPTree FindSibling(BPTree parent, int i){
	BPTree sibling = NULL;
	int limit = M;
	
	if (i == 0){
		if(parent->ptr[1]->keynum < limit){
			sibling = parent->ptr[1];
		}
	}else if(parent->ptr[i - 1]->keynum < limit){
		sibling = parent->ptr[i - 1];
	}else if(i + 1 < parent->keynum && parent->ptr[i+1]->keynum < limit){
		sibling = parent->ptr[i + 1];
	}

	return sibling;
}

/* 查找兄弟结点，其关键字树大于 M/2, 没有则返回 NULL */
static BPTree FindSiblingKeyNum_M_2(BPTree parent, int i, int *j){
	int limit = LIMIT_M_2;
	BPTree sibling = NULL;

	if(i == 0){
		if (parent->ptr[1]->keynum > limit){
			sibling = parent->ptr[1];
			*j = 1;
		}
	}else{
		if (parent->ptr[i - 1]->keynum > limit){
			sibling = parent->ptr[i - 1];
			*j = i - 1;
		}else if (i + 1 < parent->keynum && parent->ptr[i + 1]->keynum > limit){
			sibling = parent->ptr[i + 1];
			*j = i + 1;
		}
	}
	
	return sibling;
}

/**
 * 当要对position插入key的时候，i是position在parent的位置，j是key要插入的位置
 * 当要对parent插入position结点的时候， i是要插入的位置
 * */
static BPTree InsertElement(int isKey, BPTree parent, BPTree position, KeyType key, Record value, int i, int j){
	int k;
	if(isKey){
		// 插入key
		k = position->keynum - 1;
		while(k >= j){
			position->key[k + 1] = position->key[k];
			position->value[k + 1] = position->value[k];
			k--;
		}
		position->key[j] = key;
		position->value[j] = value;

		if (parent != NULL){
		//	parent->value[i] = position->value[0];
			parent->key[i] = position->key[0];
		}
		position->keynum++;
	}else{
		// 插入结点
		/* 对树叶结点进行连接 */
		if (position->ptr[0] == NULL){
			if (i > 0)
				parent->ptr[i - 1]->next = position;
			position->next = parent->ptr[i];
		}
		k = parent->keynum - 1;
		while (k >= i){
			parent->ptr[k + 1] = parent->ptr[k];
			parent->key[k + 1] = parent->key[k];
		//	parent->value[k + 1] = parent->value[k];
			k--;
		}
		parent->key[i] = position->key[0];
		parent->ptr[i] = position;
		parent->keynum++;
	}
	return position;
}

/* 删除元素，与插入类似 */
static BPTree RemoveElement(int isKey, BPTree parent, BPTree position, int i, int j){
	int k, limit;
	if (isKey){
		limit = position->keynum;
		k = j + 1;
		while(k < limit){
			position->key[k - 1] = position->key[k];
			position->value[k - 1] = position->value[k];
			k++;
		}
		position->key[position->keynum - 1] = Unavailable;
		position->value[position->keynum - 1] = Unavailable;
		parent->key[i] = position->key[0];
		position->keynum--;
	}else{
		if(position->ptr[0] == NULL && i > 0){
			parent->ptr[i - 1]->next = parent->ptr[i + 1];
		}
		limit = parent->keynum;
		k = i + 1;
		while (k < limit){
			parent->ptr[k - 1] = parent->ptr[k];
			parent->key[k - 1] = parent->key[k];
			parent->value[k - 1] = parent->value[k];
			k++;
		}
		parent->ptr[parent->keynum - 1] = NULL;
		parent->key[parent->keynum - 1] = Unavailable;
		parent->value[parent->keynum - 1] = Unavailable;

		parent->keynum--;
	}
	return position;
}
/**
 *  src 和dst 是相邻的结点，i是src在parent中的位置
 *  将 src的元素移动到dst中，n是移动元素的个数
 * */
static BPTree MoveElement(BPTree src, BPTree dst, BPTree parent, int i, int n){
	KeyType tempKey;
	Record tempRecord;
	BPTree child, temp;
	int j, srcInFront;

	srcInFront = 0;
	if (src->key[0] < dst->key[0])
		srcInFront = 1;
	j = 0;
	
	if (srcInFront){
		if (src->ptr[0] != NULL){
			while (j < n) {
				child = src->ptr[src->keynum - 1];
				temp = RemoveElement(0, src, child, src->keynum - 1, Unavailable);
			//	free(temp);
				InsertElement(0, dst, child, Unavailable, Unavailable, 0, Unavailable);
				j++;
			}
		}else{
			while (j < n){
				tempKey = src->key[src->keynum-1];
				tempRecord = src->value[src->keynum-1];
				temp = RemoveElement(1, parent, src, i, src->keynum - 1);
			//	free(temp);
				InsertElement(1, parent, dst, tempKey, tempRecord, i + 1, 0);
				j++;
			}
		}
		parent->key[i + 1] = dst->key[0];
		if(src->keynum > 0){
			FindMostRight(src)->next = FindMostLeft(dst);
		}else{
			free(src);
		}
	}else{
		if (src->ptr[0] != NULL){
			while (j < n){
				child = src->ptr[0];
				temp = RemoveElement(0, src, child, 0, Unavailable);
			//	free(temp);
				InsertElement(0, dst, child, Unavailable, Unavailable,dst->keynum, Unavailable);
				j++;
			}
		}else{
			while (j < n){
				tempKey = src->key[0];
				tempRecord = src->value[0];
				temp = RemoveElement(1, parent, src, i, 0);

				// free(temp);
				InsertElement(1, parent, dst, tempKey, tempRecord, i-1, dst->keynum);
				j++;
			}
		}

		parent->key[i] = src->key[0];
		parent->value[i] = src->value[0];
		if (src->keynum > 0){
			FindMostRight(dst)->next = FindMostLeft(src);
		}else{
			free(src);
		}
	}

	return parent;
}

static BPTree SplitNode(BPTree parent, BPTree position, int i){
	int j, k, limit;
	BPTree newNode = MallocNewNode();

	k = 0;
	j = position->keynum / 2;
	limit = position->keynum;

	while (j < limit){
		if (position->ptr[0] != NULL){
			newNode->ptr[k] = position->ptr[j];
			position->ptr[j] = NULL;
		}
		newNode->key[k] = position->key[j];
		newNode->value[k] = position->value[j];

		position->key[j] = Unavailable;
		position->value[j] = Unavailable;

		newNode->keynum++;
		position->keynum--;
		j++;
		k++;
	}
	
	if (parent != NULL){
		InsertElement(0, parent, newNode, Unavailable, Unavailable, i+1, Unavailable);
	}else{
		parent = MallocNewNode();
		InsertElement(0, parent, position, Unavailable, Unavailable, 0, Unavailable);
		InsertElement(0, parent, newNode, Unavailable, Unavailable, 1, Unavailable);
		return parent;
	}

	return position;
}

/* 合并结点， position少于 M/2 关键字， S大于等于M/2 个关键字 */
static BPTree MergeNode(BPTree parent, BPTree position, BPTree s, int i){
	int limit;

	if (s->keynum > LIMIT_M_2){
		MoveElement(s, position, parent, i, 1);
	}else{
		limit = position->keynum;
		MoveElement(position, s, parent, i, limit);
		RemoveElement(0, parent, position, i, Unavailable);

		free(position);
		position = NULL;
	}
	return parent;
}

/* 递归s插入 */
static BPTree RecursiveInsert(BPTree T, KeyType key, Record value, int i, BPTree parent){
	int j, limit;
	BPTree sibling = NULL;

	j = 0;
	while (j < T->keynum && key >= T->key[j]){
		if (key == T->key[j]){
			return T;
		}
		j++;
	}

	if (j != 0 && T->ptr[0] != NULL) j--;
	if(T->ptr[0] == NULL){
		T = InsertElement(1, parent, T, key, value, i, j);
	}else{
		T->ptr[j] = RecursiveInsert(T->ptr[j], key, value, j, T);
	}

	limit = M;

	if (T->keynum > limit){
		if (parent == NULL){
			T = SplitNode(parent, T, i);
		}else{
			sibling = FindSibling(parent, i);
			if (sibling != NULL){
				MoveElement(T, sibling, parent, i, 1);
			}else{
				T = SplitNode(parent, T, i);
			}
		}
	}

	if (parent != NULL)
		parent->key[i] = T->key[0];

	return T;
}

/* 插入 */
extern BPTree Insert(BPTree T, KeyType key, Record value){
	return RecursiveInsert(T, key, value, 0, NULL);
}

/* 递归删除 */
static BPTree RecursiveRemove(BPTree T, KeyType key, int i, BPTree parent){
	int j, needAdjust=False;
	BPTree sibling,temp;
	sibling = temp = NULL;

	/* 查找分支 */
	j = 0;
	while (j < T->keynum && key >= T->key[j]){
		if (key == T->key[j])
			break;
		j++;
	}
	
	if (j == 0){
		printf("%d not exit.\n", key);
		return T;
	}

	if (T->ptr[0] == NULL){
		if (key != T->key[j] || j == T->keynum){
			printf("%d not exit\n", key);
			return T;
		}
	}else{
		if (j == T->keynum || key < T->key[j]) j--;
	}

	// 树叶
	if (T->ptr[0] == NULL){
		T = RemoveElement(1, parent, T, i, j);
	}else{
		T->ptr[j] = RecursiveRemove(T->ptr[j], key, j, T);
	}
	
	/* 树的根或者是一个树叶，或者其儿子树在2到M之间 */
	if (parent == NULL && T->ptr[0] != NULL && T->keynum < 2)
		needAdjust = True;
	// 除根结点以外，q所有非树叶结点的儿子树在[M/2]到M之间
	else if (parent != NULL && T->ptr[0] != NULL && T->keynum < LIMIT_M_2)
		needAdjust = True;
	// 树叶结点也要满足条件
	else if (parent != NULL && T->ptr[0] == NULL && T->keynum < LIMIT_M_2)
		needAdjust = True;
	
	if (needAdjust){
		/* 根 */
		if (parent == NULL){
			if (T->ptr[0] != NULL && T->keynum < 2){
				temp = T;
				T = T->ptr[0];
				free(temp);
				return T;
			}
		}else{
			sibling = FindSiblingKeyNum_M_2(parent, i, &j);
			if (sibling != NULL){
				MoveElement(sibling, T, parent, j, 1);
			}else{
				if (i == 0)
					sibling = parent->ptr[1];
				else
					sibling = parent->ptr[i - 1];

				parent = MergeNode(parent, T, sibling, i);
				T = parent->ptr[i];
			}
		}
	}
	
	return T;
}


/* 删除 */
extern BPTree Remove(BPTree T, KeyType key){
	return RecursiveRemove(T, key, 0, NULL);
}

/* 销毁 */
extern BPTree Destroy(BPTree T){
	int i, j;
	if (T != NULL){
		i = 0;
		while (i < T->keynum + 1){
			Destroy(T->ptr[i]);
			i++;
		}
		printf("Destroy: (");
		j = 0;
		while (j < T->keynum)
			printf("%d: ", T->key[j++]);
		printf(")\n");
		free(T);
		T = NULL;
	}
	return T;
}

static void RecursiveTravel(BPTree T, int level){
	int i;
	if (T != NULL){
		printf("	");
		printf("[level: %d] -->", level);
		printf("(");
		i = 0;
		while (i < T->keynum)
			printf(" %d: ", T->key[i++]);
		printf(")\n");
		level++;

		i = 0;
		while (i <= T->keynum){
			RecursiveTravel(T->ptr[i], level);
			i++;
		}
	}
}

/* 遍历所有结点 */
extern void Travel(BPTree T){
	RecursiveTravel(T, 0);
}

/* 遍历所有叶子节点的key和value */

extern void TravelData(BPTree T){
	BPTree temp;
	int i;
	if (T == NULL)
		return;
	printf("ALL Data:\n");
	temp = T;

	while (temp->ptr[0] != NULL)
		temp = temp->ptr[0];

	while (temp != NULL){
		i = 0;
		while (i < temp->keynum){
			printf("<%d, %ld>, ", temp->key[i], temp->value[i]);
			i++;
		}
		temp = temp->next;
	}
}

extern Result* SearchBPTree(BPTree T, KeyType key){
	int i=0;
	Result *res = NULL;
        res =(Result*)malloc(sizeof(Result));
	BPTree p = NULL;

	BPTree temp = T;
	int found = False;

	res->pt = NULL;
	res->i = Unavailable;
	res->tag = False;

	while(temp && !found){
		i = 0;
		while(i < temp->keynum && temp->key[i] <= key){
 			if ((key == temp->key[i]) && (temp->ptr[0] == NULL)){
				found = True;
				break;
			}
			i++;
		}
		if(0 != i && temp->ptr[0] != NULL) i--;
		if (found){
			res->pt = temp;
			res->i = i;
			res->tag = True;
			break;
		}else{
			temp = temp->ptr[i];
		}

	}

	return res;
}
