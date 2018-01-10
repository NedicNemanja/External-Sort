#include "HelperFunctions.h"

void QuickSortRun(BF_Block** blockArray, int size, int fieldNo){

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
