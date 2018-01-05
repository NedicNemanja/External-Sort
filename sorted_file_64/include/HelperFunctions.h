#ifndef HELPERFUNCTIONS_H
#define HELPERFUNCTIONS_H

#include "bf.h"

/*store all the runs from pinnedBlocks to fileDesc,
also unpins all blocks from pinnedBlocks*/
void StoreRun(int fileDesc,BF_Block* pinnedBlocks,int run_size);

#endif
