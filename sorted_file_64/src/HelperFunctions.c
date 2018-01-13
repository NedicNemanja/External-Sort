#include "heap.h"
#include "HelperFunctions.h"
#include "Run.h"
#include "structs.h"

//function to decrease an index struct of our quicksort
Index indexDecr(Index index){
	//if its the first record of this block
	if (index.recordIndex == 0){
		/*if (index.blockIndex == 0)
		{
			printf("Error in Quicksort's index decrease.\n");
			exit(-1);
		}else{
			lastRecordIndex = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD

			index.blockIndex--;
			index.recordIndex = lastRecordIndex;
		}*/
		//Set the block index to the previous one and the record index to the last of the previous block
		int lastRecordIndex = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD - 1;
		index.blockIndex--;
		index.recordIndex = lastRecordIndex;
	}
  else{
		//else if its a random record just set it to the previous one
		index.recordIndex--;
	}
	return index;
}
//function to inrease an index struct of our quicksort
Index indexIncr(Index index, int size){
	//calculate the index number of the last record ([0..n-1])
	//AUTO EINAI LATHOS
	int lastRecordIndex = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD - 1;
	//if the index to increase is the last record of a block
	if (index.recordIndex == lastRecordIndex){	//and the block is the last block
		/*if (index.blockIndex == size - 1){//then we cannot increase it without getting out of borders
			//printf("Error in Quicksort's index increase %d %d.\n", index.blockIndex, index.recordIndex);
			exit(-1);
		}
    else{//else set the block to the next and the record to the first of the next
			index.blockIndex++;
			index.recordIndex = 0;
		}*/
		index.blockIndex++;
		index.recordIndex = 0;
	}
  else{//else if its just a random record set it to the next
		index.recordIndex++;
	}
	return index;
}
//function to get the record with index requesteRecord from the requested block ([0..n-1]) from the blockArray with size size
Record getRecordFromBlock(BF_Block** blockArray, int size, int requestedBlock, int requestedRecord){
	//if the requested block is bigger than the amount of available
	if (requestedBlock >= size)
	{
		printf("Error in Quicksort's getRecordFromBlock - requestedBlock doesnt exist.\n");
		exit(-1);
	}

	Record tmpRecord;
	char * data = NULL;
	int offset = BLOCKBASEOFFSET + requestedRecord*SIZEOFRECORD;

	//printf("BLOCK: %d RECORD: %d OFFSET: %d\n", requestedBlock, requestedRecord, offset);

	data = BF_Block_GetData(blockArray[requestedBlock]);
	memmove(&(tmpRecord.id), data + offset, sizeof(int));
	offset+=sizeof(int);
	memmove(&(tmpRecord.name),data + offset, SIZEOFNAME);
	offset+=SIZEOFNAME;
	memmove(&(tmpRecord.surname),data + offset, SIZEOFSURNAME);
	offset+=SIZEOFSURNAME;
	memmove(&(tmpRecord.city),data + offset, SIZEOFCITY);
	return tmpRecord;
}
//function to set the record with index targetRecord from the targetBlock to the value of record
void setRecord(BF_Block** blockArray, int targetBlock, int targetRecord, Record record){

	char * data = NULL;
	int offset = BLOCKBASEOFFSET + targetRecord*SIZEOFRECORD;

	data = BF_Block_GetData(blockArray[targetBlock]);
	memmove(data + offset, &(record.id), sizeof(int));
	offset+=sizeof(int);
	memmove(data + offset, &(record.name), SIZEOFNAME);
	offset+=SIZEOFNAME;
	memmove(data + offset, &(record.surname), SIZEOFSURNAME);
	offset+=SIZEOFSURNAME;
	memmove(data + offset, &(record.city), SIZEOFCITY);
}
//function to check if tmpRecord is less ot equal than pivot based on their fieldno'th attribute
int recordLessEqualThan(Record tmpRecord, Record pivot, int fieldNo){
	switch (fieldNo){
		case 0:
			if (tmpRecord.id <= pivot.id)
				return 1;
			return 0;
		case 1:
			if (strcmp(tmpRecord.name, pivot.name) <= 0)
				return 1;
			return 0;
		case 2:
			if (strcmp(tmpRecord.surname, pivot.surname) <= 0)
				return 1;
			return 0;
		case 3:
			if (strcmp(tmpRecord.city, pivot.city) <= 0)
				return 1;
			return 0;
	}
}

int recordLessThan(Record *tmpRecord, Record *pivot, int fieldNo){
	switch (fieldNo){
		case 0:
			if (tmpRecord->id < pivot->id)
				return 1;
			return 0;
		case 1:
			if (strcmp(tmpRecord->name, pivot->name) < 0)
				return 1;
			return 0;
		case 2:
			if (strcmp(tmpRecord->surname, pivot->surname) < 0)
				return 1;
			return 0;
		case 3:
			if (strcmp(tmpRecord->city, pivot->city) < 0)
				return 1;
			return 0;
	}
}

//function to swap the values of the record with index i with the record with index j
void recordSwap(BF_Block** blockArray, int size, Index i, Index j){
	//get the two records in two temp variables
	Record tmpReci = getRecordFromBlock(blockArray, size, i.blockIndex, i.recordIndex);
	Record tmpRecj = getRecordFromBlock(blockArray, size, j.blockIndex, j.recordIndex);
	//and se each one to each others position
	setRecord(blockArray, i.blockIndex, i.recordIndex, tmpRecj);
	setRecord(blockArray, j.blockIndex, j.recordIndex, tmpReci);
}

//function of a typical Quicksort function that gathers all the smaller items than pivot to its left and
//all the bigger to its right. Pivot is selected to be every time the last element of the array
Index partition(BF_Block** blockArray, int size, int fieldNo, Index low, Index high){
	int b, r;
	printf("MPIKA me apo %db %dr ews %db %dr\n", low.blockIndex, low.recordIndex, high.blockIndex, high.recordIndex);

	Record pivot = getRecordFromBlock(blockArray, size, high.blockIndex, high.recordIndex);
	Index i = indexDecr(low);
	int numRecs = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD;
	//Iterate through all records from low to high

	for (b = low.blockIndex; b <= high.blockIndex; b++){
		if(b == low.blockIndex && b == high.blockIndex){
			for (r = low.recordIndex; r <= high.recordIndex; r++){
	    		if (b == high.blockIndex && r > (high.recordIndex - 1))
	    		{
	    			break;
	    		}
	      		Record tmpRec = getRecordFromBlock(blockArray, size, b, r);
					//if the current record is smaller or equal to pivot
				if (recordLessEqualThan(tmpRec, pivot, fieldNo)){
					//printf("1\n");
					i = indexIncr(i, size);
					Index j;
					j.blockIndex = b;
					j.recordIndex = r;
					//then swap them
					recordSwap(blockArray, size, i, j);
				}
			}
		}else if(b == low.blockIndex){
			for (r = low.recordIndex; r <= numRecs - 1; r++){

	      		Record tmpRec = getRecordFromBlock(blockArray, size, b, r);
					//if the current record is smaller or equal to pivot
				if (recordLessEqualThan(tmpRec, pivot, fieldNo)){
					//printf("1\n");
					i = indexIncr(i, size);
					Index j;
					j.blockIndex = b;
					j.recordIndex = r;
					//then swap them
					recordSwap(blockArray, size, i, j);
				}
			}
		}
		else if (b == high.blockIndex){
			for (r = low.recordIndex; r <= high.recordIndex; r++){
				if (b == high.blockIndex && r > (high.recordIndex - 1)){
	    			break;
	    	}

	      		Record tmpRec = getRecordFromBlock(blockArray, size, b, r);
					//if the current record is smaller or equal to pivot
				if (recordLessEqualThan(tmpRec, pivot, fieldNo)){
					//printf("1\n");
					i = indexIncr(i, size);
					Index j;
					j.blockIndex = b;
					j.recordIndex = r;
					//then swap them
					recordSwap(blockArray, size, i, j);
				}
			}
		}
		else{
			for (r = 0; r <= numRecs - 1; r++){

	      		Record tmpRec = getRecordFromBlock(blockArray, size, b, r);
					//if the current record is smaller or equal to pivot
				if (recordLessEqualThan(tmpRec, pivot, fieldNo)){
					//printf("1\n");
					i = indexIncr(i, size);
					Index j;
					j.blockIndex = b;
					j.recordIndex = r;
					//then swap them
					recordSwap(blockArray, size, i, j);
				}
			}
		}
	}
	//printf("2\n");
	i = indexIncr(i, size);
	recordSwap(blockArray, size, i, high);
	return i;
}
//LOW IS 0 HIGH IS MAX - 1
void QuickSortRun(BF_Block** blockArray, int size, int fieldNo, Index low, Index high){

	if (low.blockIndex <= high.blockIndex && low.recordIndex < high.recordIndex){
		//partition the array with part as seperator, after this the blockArray[part] is in the correct sorted position
		Index part = partition(blockArray, size, fieldNo, low, high);

		Index decreasedPart = indexDecr(part);
		//printf("3\n");
		Index increasedPart = indexIncr(part, size);
		//now sort in the same way the elements in the left of part and to the right of part
		QuickSortRun(blockArray, size, fieldNo, low, decreasedPart);
		QuickSortRun(blockArray, size, fieldNo, increasedPart, high);
	}
}

void quickSort(BF_Block** blockArray, int size, int fieldNo, int lastRun, int lastRunSize){
	Index low, high;
	char *data = NULL;

	low.blockIndex = 0;
	low.recordIndex = 0;

	if(lastRun){
		high.blockIndex = lastRunSize - 1;

		data = BF_Block_GetData(blockArray[lastRunSize-1]);
		memmove(&(high.recordIndex), data, sizeof(int));
		high.recordIndex--;
	}
	else{
		high.blockIndex = size - 1;
		high.recordIndex = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD - 1;
	}

	QuickSortRun(blockArray, size ,fieldNo, low, high);
}

int isFull(BF_Block *block){
	int recs =0;
	char * data = BF_Block_GetData(block);
	memmove(&recs, data, sizeof(int));
	int avail_space = BF_BLOCK_SIZE - BLOCKBASEOFFSET-(recs+1)*SIZEOFRECORD;
	if(avail_space < 0) return 1;
	else return 0;
}

int isFinished(BF_Block *block, int offset){
	char * data = BF_Block_GetData(block);
	int recs =0;
	memmove(&recs, data, sizeof(int));
	printf("Recs are %d\n", recs);
	fflush(stdout);
	int past = offset - (recs*SIZEOFRECORD)- BLOCKBASEOFFSET;
	printf("past is %d\n", past);
	if(past >= 0) return 1;
	else return 0;
}

//initialize offsets array with specific offset
void InitOffsets(int **offsets, int size, int offset){
	*offsets = malloc(size*sizeof(int));
	for(int i=0; i<size; i++)
		(*offsets)[i] = offset;
}

void initBlock(BF_Block *block){
	char *data = NULL;
	data = BF_Block_GetData(block);
	int offset = 0;
	int size = 0;
  //move the counter (0) to the start of the block
  memmove(data, &offset, sizeof(int));
  offset = sizeof(int);
  //move sizes of Record to the start of the block
  size = SIZEOFID;
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  size = SIZEOFNAME;
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  size = SIZEOFSURNAME;
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  size = SIZEOFCITY;
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
}

//NEME VALE ENA FILE EDO NA GRAFO
void SortAndStoreRuns(Run** runArray, int size, int fieldNo, int out_fileDesc){
	int *offsets = NULL, end=0, min, recs;
	char *target = NULL;
	BF_Block *outBlock = NULL;
	InitOffsets(&offsets, size+1, BLOCKBASEOFFSET);
	BF_Block_Init(&outBlock);
	//initialize the first block
	//BF_AllocateBlock(out_fileDesc, outBlock);
	//initFirstBlock(outBlock);

	BF_AllocateBlock(out_fileDesc, outBlock);
	initBlock(outBlock);
	//heap theheap;
	//makeheap(blockArray, size-1, fieldNo, &theheap);
	//while(theheap.size >0){
	for(int i=0; i<size; i++){
		printf("array[%d] has size %d\n", i, runArray[i]->size);
	}
	fflush(stdout);

	while(!end){
		//check if last buffer is full
		if(isFull(outBlock)){
			//printf("Block is full\n");
			//allocate a new one
			BF_Block_SetDirty(outBlock);
			BF_UnpinBlock(outBlock);
			BF_AllocateBlock(out_fileDesc, outBlock);
			initBlock(outBlock);
			offsets[size] = BLOCKBASEOFFSET;
		}

		min = 0;
		//find a min and if you don't find, break;
		while(min <size){
			if(runArray[min]->size){
				if(isFinished(runArray[min]->pinnedBlock, offsets[min])){
					printf("Is finished22!, min %d\n", min);
					fflush(stdout);
					Run_NextBlock(runArray[min]);
					offsets[min] = BLOCKBASEOFFSET;
				}
			}
			if(!runArray[min]->size)
				min++;
			else
				break;
		}
		if(min >= size) break;
		//printf("Found min %d, run->size %d, offset: %d\n", min, runArray[min]->size, offsets[min]);
		Record *minRec = (Record *) (BF_Block_GetData(runArray[min]->pinnedBlock) + offsets[min]);
		//printf("minRec %d %s %s %s\n", minRec->id, minRec->name, minRec->surname, minRec->city);
		for(int i=min+1; i<size; i++){
			//printf("NE\n");
			//fflush(stdout);
			if(!runArray[i]->size){
				continue;
			}
			else{
				printf("offsets[i]: %d\n", offsets[i]);
				//check here if block is at end
				printf("i is %d, size is %d, Run is %d\n", i, size, runArray[i]->size);
				if(isFinished(runArray[i]->pinnedBlock, offsets[i])){
					//printf("Is finished!\n");
					//fflush(stdout);
					Run_NextBlock(runArray[i]);
					offsets[i] = BLOCKBASEOFFSET;
				}
				if(runArray[i]->size){
					Record *tRec = (Record *) (BF_Block_GetData(runArray[i]->pinnedBlock) + offsets[i]);
					//printf("tRec %d %s %s %s\n", tRec->id, tRec->name, tRec->surname, tRec->city);
					fflush(stdout);
					//printf("offsets[min]: %d, offsets[i]: %d, offsets[size]: %d\n", offsets[min], offsets[i], offsets[size]);
					fflush(stdout);
					if(recordLessThan(tRec, minRec, fieldNo)){
						min = i;
						minRec = tRec;
					}
				}
			}
		}

		target = BF_Block_GetData(outBlock);
		//move the record to the sorted buffer and increment offsets
		printf("Of[m] %d, min %d size %d\n", offsets[min], min, size);
		printf("runArray[min]->size %d\n", runArray[min]->size);
		fflush(stdout);
		printf("%d %s %s %s\n", minRec->id, minRec->name, minRec->surname, minRec->city);
		fflush(stdout);
		memmove(target+offsets[size], minRec, SIZEOFRECORD);
		offsets[min] += SIZEOFRECORD;
		offsets[size] += SIZEOFRECORD;
		//increment the number of recs in buffer
		memmove(&recs, target, sizeof(int));
		recs++;
		memmove(target, &recs, sizeof(int));
	}

		//THIS IS HEAPSORT
		//writeroot
		//swap(theheap, 0, theheap.size-1);
		//insert from index = theheap.nodes[theheap.size-1].i
		//if(has_record(blockArray[index]){
			//insert next to heap
		//}
		//else if(next_block){
			//bring next from run and insert to heap
		//}
		//else{
				//theheap.size--;
		//}
	printf("Bye bye cruel world!\n");
	fflush(stdout);
	for(int i=0; i<size; i++){
		printf("array[%d] has size %d\n", i, runArray[i]->size);
	}
	fflush(stdout);
	//cleanup
	//destroyheap(&theheap);
	BF_Block_SetDirty(outBlock);
	BF_UnpinBlock(outBlock);
	BF_Block_Destroy(&outBlock);
	free(offsets);
}

int CopyFile(int fileDesc1, int fileDesc2){
	int blocks = 0, offset = 0, recs = 0;
	char *data1 = NULL, *data2 = NULL;
	BF_Block *block1 = NULL, *block2 = NULL;
	int id_size = 0;
	int name_size = 0;
	int sur_size = 0;
	int city_size = 0;
	BF_ErrorCode err;
	BF_Block_Init(&block1);
	BF_Block_Init(&block2);

	BF_GetBlockCounter(fileDesc1, &blocks);

	//for all blocks
	for(int i=1; i<blocks; i++){
		if(BF_GetBlock(fileDesc1, i, block1) != BF_OK)
			fprintf(stderr, "Something went wrong\n");
		if(BF_AllocateBlock(fileDesc2, block2) != BF_OK)
			fprintf(stderr, "Something went wrong2\n");
		offset = 0;
		//get number of recs in the block
		data1 = BF_Block_GetData(block1);
		data2 = BF_Block_GetData(block2);
		memmove(&recs, data1, sizeof(int));
		memmove(data2, &recs, sizeof(int));
		offset = sizeof(int);
		//get sizes of Record
		memmove(&id_size, data1+offset, sizeof(int));
		memmove(data2+offset, data1+offset, sizeof(int));
		offset += sizeof(int);
		memmove(&name_size, data1+offset, sizeof(int));
		memmove(data2+offset, data1+offset, sizeof(int));
		offset += sizeof(int);
		memmove(&sur_size, data1+offset, sizeof(int));
		memmove(data2+offset, data1+offset, sizeof(int));
		offset += sizeof(int);
		memmove(&city_size, data1+offset, sizeof(int));
		memmove(data2+offset, data1+offset, sizeof(int));
		offset += sizeof(int);

		//for each block print the entries
		for(int j=0; j< recs; j++){
			//print id
			memmove(data2+offset, data1+offset, id_size);
			offset += id_size;
			//print name
			memmove(data2+offset, data1+offset, name_size);
			offset += name_size;
			//print surname
			memmove(data2+offset, data1+offset, sur_size);
			offset += sur_size;
			//print city
			memmove(data2+offset, data1+offset, city_size);
			offset += city_size;
		}
		BF_Block_SetDirty(block2);
		BF_UnpinBlock(block2);
		BF_UnpinBlock(block1);
	}
	BF_Block_Destroy(&block2);
	BF_Block_Destroy(&block1);
	printf("File1 blocks: %d\n", blocks);
	BF_GetBlockCounter(fileDesc2, &blocks);
	printf("File2 blocks: %d\n", blocks);
	return 1;
}


int PinGroup(Run** pinnedRuns,int in_file,int* current_block_id,int run_size,
							int* num_of_unmerged_blocks,int lastRunSize, int bufferSize)
{
	int group_size=0;
	for(int buffer_index=0; buffer_index<bufferSize-1; buffer_index++){
		//if there are more runs to merge
		if(*num_of_unmerged_blocks > 0){
			//if this is a regular run
			if(*num_of_unmerged_blocks > lastRunSize){
				pinnedRuns[buffer_index] = Run_init(in_file,*current_block_id,run_size);
				group_size++;
				//mark this run as merged
				*num_of_unmerged_blocks -= run_size;
				//look ahead
				*current_block_id += run_size;
			}
			//if this is the last run
			else{
				*current_block_id -= run_size;
				*current_block_id += lastRunSize;
				pinnedRuns[buffer_index] = Run_init(in_file,*current_block_id,lastRunSize);
				group_size++;
				*num_of_unmerged_blocks -= lastRunSize;
			}
		}
		//nothing left to merge in this group
		else
			return group_size;
	}
}

void UnpinGroup(Run** pinnedRuns,int num_of_runs){
	for(int i=0; i<num_of_runs;i++){
		Run_destroy(pinnedRuns[i]);
		pinnedRuns[i] = NULL;
	}
}
