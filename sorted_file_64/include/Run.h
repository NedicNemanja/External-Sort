#ifndef RUN_H
#define RUN_H

#include "bf.h"

typedef struct Run{
  BF_Block* pinnedBlock;  //every Run at any moment has only one if its blocks in the buffer
  int pinnedBlock_id;
  int size;               //the number of blocks this run contains
  int fileDesc;
}Run;

Run* Run_init(int fileDesc, int current_block_id, int run_size);

void Run_NextBlock(Run*);

void Run_destroy(Run*);

#endif
