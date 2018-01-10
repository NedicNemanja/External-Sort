#include "heap.h"
#include "HelperFunctions.h"

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
	int lastRecordIndex = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD - 1;
	//if the index to increase is the last record of a block
	if (index.recordIndex == lastRecordIndex){	//and the block is the last block
		if (index.blockIndex == size - 1){//then we cannot increase it without getting out of borders
			printf("Error in Quicksort's index increase.\n");
			exit(-1);
		}
    else{//else set the block to the next and the record to the first of the next
			index.blockIndex++;
			index.recordIndex = 0;
		}
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

	data = BF_Block_GetData(blockArray[requestedBlock]);
	memmove(&(tmpRecord.id),data + offset, sizeof(int));
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
//function to swap the values of the record with index i with the record with index j
void recordSwap(BF_Block** blockArray, int size, Index i, Index j){
	//get the two records in two temp variables
	Record tmpReci = getRecordFromBlock(blockArray, size, i.blockIndex, i.recordIndex);
	Record tmpRecj = getRecordFromBlock(blockArray, size, j.blockIndex, j.recordIndex);
	//and se each one to each others position
	setRecord(blockArray, i.blockIndex, i.recordIndex, tmpRecj);
	setRecord(blockArray, j.blockIndex, j.recordIndex, tmpReci);
}
//function of a typical Quicksort function that gathers all the smaller items than pivot to its lefta and
//all the bigger to its right. Pivot is selected to be every time the last element of the array
Index partition(BF_Block** blockArray, int size, int fieldNo, Index low, Index high){
	int b, r;

	Record pivot = getRecordFromBlock(blockArray, size, high.blockIndex, high.recordIndex);
	Index i = indexDecr(low);
	//Iterate through all records from low to high
	for (b = low.blockIndex; b <= high.blockIndex; b++){
    for (r = low.recordIndex; r <= high.recordIndex - 1; r++){
      Record tmpRec = getRecordFromBlock(blockArray, size, b, r);
				//if the current record is smaller or equal to pivot
			if (recordLessEqualThan(tmpRec, pivot, fieldNo)){
				i = indexIncr(i, size);
				Index j;
				j.blockIndex = b;
				j.recordIndex = r;
				//then swap them
				recordSwap(blockArray, size, i, j);
			}
		}
	}
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
		Index increasedPart = indexIncr(part, size);
		//now sort in the same way the elements in the left of part and to the right of part
		QuickSortRun(blockArray, size, fieldNo, low, decreasedPart);
		QuickSortRun(blockArray, size, fieldNo, increasedPart, high);
	}
}

int isfull(BF_Block *block){
	//check here if block is full
	//NEME
}

//initialize offsets array with specific offset
void InitOffsets(int *offsets, int size, int offset){
	offsets = malloc(size*sizeof(int));
	for(int i=size=1; i>=0; i--)
		offsets[i] = offset;
}

//NEME VALE ENA FILE EDO NA GRAFO
void HeapSortRun(BF_Block** blockArray, int size, int fieldNo /*,file*/){
	int *offsets = NULL;
	InitOffsets(offsets, size-1, BLOCKBASEOFFSET);
	heap theheap;
	//makeheap(blockArray, size-1, fieldNo, &theheap);
	while(theheap.size >0){
		//check if last buffer is full
		if(isfull(blockArray[size-1])){
			//flush
		}
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
	}
	//destroyheap(&theheap);
	free(offsets);
}


void StoreRun(int fileDesc, BF_Block** pinnedBlocks, int run_size){
  //store and unpin this run block by block
  for(int i=0; i<run_size; i++){
    InsertBlock(fileDesc, pinnedBlocks[i]);//insert this blocks records the the file
    BF_UnpinBlock(pinnedBlocks[i]);
    pinnedBlocks[i] = NULL;
  }
}

void InsertBlock(int fileDesc, BF_Block* block){

}
