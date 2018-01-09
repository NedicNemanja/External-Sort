#include "HelperFunctions.h"

void QuickSortRun(BF_Block** blockArray, int size, int fieldNo){

}

void swap(int *blockArray, int x, int y){
  int temp = blockArray[x];
  blockArray[x] = blockArray[y];
  blockArray[y] = temp;
}

void heapify(int *blockArray, int n, int i){
  //int size =0; //should be REMOVED
  int max = i, l = (max << 1) +1, r = (max + 1) << 1;

  if(l < n && blockArray[l] > blockArray[max])
    max = l;

  if(r < n && blockArray[r] > blockArray[max])
    max = r;

  if(max != i){
    swap(blockArray, i, max);
    heapify(blockArray, n, max);
  }
}

void makeheap(){

}

void HeapSortRun(BF_Block** blockArray, int size, int fieldNo){

}


void StoreRun(int fileDesc, BF_Block** pinnedBlocks, int run_size){
  //store and unpin this run block by block
  for(int i=0; i<run_size; i++){
    InsertBlock(fileDesc, pinnedBlocks[i]);//insert this blocks records the the file
    BF_UnpinBlock(pinnedBlocks[i]);
    pinnedBlocks[i] = NULL;
  }
}

void InsertBlock(int fileDesc, BF_Block* block){

}
