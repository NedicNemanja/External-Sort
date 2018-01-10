#include "Run.h"
#include <stdlib.h>

Run* Run_init(int fileDesc, int current_block_id, int run_size){
  Run* run = malloc(sizeof(Run));
  BF_GetBlock(fileDesc,current_block_id,run->pinnedBlock);
  run->size = run_size;
}

void Run_NextBlock(){

}

void Run_destroy(){

}
