#include <stdlib.h>

#include "Run.h"

Run* Run_init(int fileDesc, int current_block_id, int run_size){
  Run* run = malloc(sizeof(Run));
  BF_Block_Init(&(run->pinnedBlock));
  BF_GetBlock(fileDesc,current_block_id,run->pinnedBlock);
  run->size = run_size;
  run->pinnedBlock_id = current_block_id;
  return run;
}

void Run_NextBlock(Run* run){
  BF_UnpinBlock(run->pinnedBlock);
  if(--size ==0){
    return;
  }
  run->pinnedBlock_id++;
  BF_GetBlock(run->fileDesc, run->pinnedBlock_id, run->pinnedBlock);
}

void Run_destroy(Run* run){
  BF_Block_Destroy(&(run->pinnedBlock));
  free(run);
}
