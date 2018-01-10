#ifndef HEAP_H
#define HEAP_H

#include "bf.h"
#include "structs.h"

typedef struct heapnode{
  int i; //where this node came from
  char * data;
  int offset;
  Index index;
}heapnode;

typedef struct heap{
  int size;
  heapnode *nodes;
}heap;

void makeheap(BF_Block** array, int size, int fieldNo, heap* theheap);

void destroyheap(heap* theheap);

#endif
