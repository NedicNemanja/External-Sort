#ifndef HELPERFUNCTIONS_H
#define HELPERFUNCTIONS_H

#include "bf.h"
#include <stdlib.h>
#include "stdio.h"
#include <string.h>
#include "structs.h"

#define BLOCKBASEOFFSET (5 * sizeof(int))
#define SIZEOFID (sizeof(int))
#define SIZEOFNAME 15
#define SIZEOFSURNAME 20
#define SIZEOFCITY 20
#define SIZEOFRECORD (sizeof(int) + 15 + 20 + 20)

/*Sorts all the records of all the blocks in blockArray[size] based on the
attribute selected by fieldNo.
No more blocks should be allocated.
Just the records should be moved around until the array is sorted.
After this function StoreRun should be called to store the sorted run back to
the disk.*/
void QuickSortRun(BF_Block** blockArray, int size, int fieldNo, Index low, Index high);

/*Sorts blockArray[size] base on the selected fieldNo of the record*/
void HeapSortRun(BF_Block** blockArray, int size, int fieldNo);

/*store all the runs from pinnedBlocks to fileDesc,
also unpins all blocks from pinnedBlocks*/
void StoreRun(int fileDesc,BF_Block** pinnedBlocks,int run_size);

/*insert a whole block into the described file*/
void InsertBlock(int fileDesc, BF_Block* block);

#endif