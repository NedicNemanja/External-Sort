#ifndef HELPERFUNCTIONS_H
#define HELPERFUNCTIONS_H

#include "bf.h"

/*sorts pinnedBlocks[bufferSize] base on the selected fieldNo of the record*/
void HeapSortRun(BF_Block** pinnedBlocks, int bufferSize, int fieldNo);

/*store all the runs from pinnedBlocks to fileDesc,
also unpins all blocks from pinnedBlocks*/
void StoreRun(int fileDesc,BF_Block** pinnedBlocks,int run_size);

/*insert a whole block into the described file*/
void InsertBlock(int fileDesc, BF_Block block);

#endif
