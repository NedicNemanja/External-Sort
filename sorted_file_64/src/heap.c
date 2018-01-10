#include <stdlib.h>

#include "heap.h"
#include "bf.h"

void init_heapnode(heapnode *node, int i, char *data, int offset){
  node->i = i;
  node->data = data;
  node->offset = offset;
}

//this swap needs to swap 2 records
void swap(heap *theheap, int x, int y){
  //athlios pseudokodikas alla neti
  char * temp = theheap->nodes[x]->data+offset;
  theheap->nodes[x] = theheap->nodes[y];
  theheap->nodes[y] = temp;
}

//THANASARA VALE KANA COMPARE EDO
void heapify(heap *theheap, int n, int i, int fieldNo){
  int max = i, l = (max << 1) +1, r = (max + 1) << 1;
  //I need a compare function here
  if(l < n && theheap->nodes[l]->data+offset > theheap->nodes[max]->data+offset)
    max = l;

  if(r < n && theheap->nodes[r]->data+offset > theheap->nodes[max]->data+offset)
    max = r;

  if(max != i){
    swap(theheap, i, max);
    heapify(theheap, n, max, fieldNo);
  }
}

void makeheap(BF_Block** array, int size, int fieldNo, heap* theheap){
  theheap->nodes = malloc(size*sizeof(heapnode));
  theheap->size = size;
  //intialize all the nodes
  for(int i=0; i<size; i++){
    init_heapnode(theheap->nodes[i], i, BF_Block_GetData(array[i]), 5*sizeof(int));
  }
  //heapify from the bottom up
  for(int i=(theheap->size-2)/2; i>=0; i--)
    heapify(theheap, theheap->size-1, i, fieldNo);
}

void destroyheap(heap* theheap){
  free(theheap->nodes);
}
