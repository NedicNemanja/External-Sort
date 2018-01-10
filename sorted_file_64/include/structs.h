#ifndef STRUCTS_H
#define STRUCTS_H
typedef struct Record {
	int id;
	char name[15];
	char surname[20];
	char city[20];
} Record;

typedef struct Index
{
	int blockIndex;
	int recordIndex;
}Index;
#endif
