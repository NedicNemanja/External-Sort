#include <stdlib.h>
#include <stdio.h>
#include "Run.h"

Run* Run_init(int fileDesc, int current_block_id, int run_size){
  Run* run = malloc(sizeof(Run));
  BF_Block_Init(&(run->pinnedBlock));
  BF_GetBlock(fileDesc,current_block_id,run->pinnedBlock);
  run->size = run_size;
  run->pinnedBlock_id = current_block_id;
  run->fileDesc = fileDesc;
printf("CREATING RUN WITH 1block:%d,run_size:%d--------\n", current_block_id,run_size);
  return run;
}

void Run_NextBlock(Run* run){
  BF_UnpinBlock(run->pinnedBlock);
  printf("Size is % d ", run->size);
  printf("id of prev block is %d ", run->pinnedBlock_id);
  //run ended
  run->size--;
  if(run->size == 0){
	 return;
 }
  else{
    run->pinnedBlock_id++;
    BF_GetBlock(run->fileDesc, run->pinnedBlock_id, run->pinnedBlock);
  }
  printf("id of new block is %d \n", run->pinnedBlock_id);
}

void Run_destroy(Run* run){
  BF_Block_Destroy(&(run->pinnedBlock));
  free(run);
}
