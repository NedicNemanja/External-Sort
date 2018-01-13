#ifndef HELPERFUNCTIONS_H
#define HELPERFUNCTIONS_H

#include "bf.h"
#include <stdlib.h>
#include "stdio.h"
#include <string.h>
#include "structs.h"
#include "Run.h"
#include <sys/sendfile.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

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
*/
void QuickSortRun(BF_Block** blockArray, int size, int fieldNo, Index low, Index high);

void quickSort(BF_Block** blockArray, int size, int fieldNo, int lastRun, int lastRunSize);

/*Sorts blockArray[size] base on the selected fieldNo of the record*/
void SortAndStoreRuns(Run** pinnedRuns,int num_of_runs,int fieldNo,int out_file);

//copy the file with name inputFileName to a new file it creates, the outputFileName, returns the fd of the output file
int CopyFile(int fileDesc1, int fileDesc2);


//load a group of runs to the buffer
int PinGroup(Run** pinnedRuns,int in_file,int* current_block_id,int run_size,
                  int* num_of_unmerged_blocks,int lastRunSize,int bufferSize);

//undoes PinGroup()
void UnpinGroup(Run** pinnedRuns,int num_of_runs);

#endif
