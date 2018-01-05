#include "HelperFunctions.h"

void StoreRun(int fileDesc, BF_Block* pinnedBlocks, int run_size){
  //store and unpin this run block by block
  for(int i=0; i<bufferSize; i++){
    InsertBlock(fileDesc,pinnedBlocks[i]);//insert this blocks records the the file
    BF_UnpinBlock(pinnedBlocks[i]);
    pinnedBlocks[i] = NULL;
  }
}
