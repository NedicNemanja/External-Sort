#ifndef HEAP_H
#define HEAP_H

typdef struct heapnode{
  unsigned int i;
  char * data;
  unsigned int offset;
}heapnode;

typedef struct heap{
  unsigned int size;
  heapnode * nodes;
}heap;

#endif
