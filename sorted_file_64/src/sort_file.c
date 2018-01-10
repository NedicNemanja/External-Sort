#include <string.h>
#include "sort_file.h"
#include "bf.h"
#include "HelperFunctions.h"

SR_ErrorCode SR_Init() {
  // Your code goes here

  return SR_OK;
}

SR_ErrorCode SR_CreateFile(const char *fileName) {
  // Your code goes here
  BF_Block * block = NULL;
  char *data = NULL;
  char * message = "Sort";
  int fileDesc = 0;
  if(BF_CreateFile(fileName) != BF_OK)
    return SR_ERROR;

  if(BF_OpenFile(fileName, &fileDesc) != BF_OK)
   return SR_ERROR;
  BF_Block_Init(&block);
  //allocate first block
  if(BF_AllocateBlock(fileDesc, block) != BF_OK)
    return SR_ERROR;
  //get pointer to block data
  data = BF_Block_GetData(block);
  //write "sort" to know that it's a sort file
  memmove(data, message, sizeof(message));
  //cleanup
  BF_Block_SetDirty(block);
  if(BF_UnpinBlock(block) != BF_OK)
    return SR_ERROR;
  BF_Block_Destroy(&block);
  if(BF_CloseFile(fileDesc) != BF_OK)
    return SR_ERROR;
  return SR_OK;
}

SR_ErrorCode SR_OpenFile(const char *fileName, int *fileDesc) {
  // Your code goes here
  BF_Block * block = NULL;
  char *data = NULL;
  char message [5];
  BF_Block_Init(&block);

  if(BF_OpenFile(fileName, fileDesc) != BF_OK)
    return SR_ERROR;
  //get block 0 to confirm it's a sort file
  if(BF_GetBlock(*fileDesc, 0, block) != BF_OK)
    return SR_ERROR;
  //get pointer to block data
  data = BF_Block_GetData(block);
  memmove(message, data, sizeof(message));
  if(strcmp(message,"Sort"))
    return SR_ERROR;
  if(BF_UnpinBlock(block) != BF_OK)
    return SR_ERROR;
  BF_Block_Destroy(&block);
  return SR_OK;
}

SR_ErrorCode SR_CloseFile(int fileDesc) {
  // Your code goes here
  if(BF_CloseFile(fileDesc) != BF_OK)
    return SR_ERROR;
  return SR_OK;
}

//helper method that copies the record counter and
//the sizes of record in a new block
int init_block(char * data, Record * record){
  int offset = 0;
  //size gets sizes so we can pass its address to memmove
  int size = 0;
  //move the counter (0) to the start of the block
  memmove(data, &offset, sizeof(int));
  offset = sizeof(int);
  //move sizes of Record to the start of the block
  size = sizeof(record->id);
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  size = sizeof(record->name);
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  size = sizeof(record->surname);
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  size = sizeof(record->city);
  memmove(data+offset, &size, sizeof(int));
  offset += sizeof(int);
  return offset;
}

SR_ErrorCode SR_InsertEntry(int fileDesc,	Record record) {
  // Your code goes here

  int offset = 0, blocks = 0, recs = 0;
  int avail_space = 0, recsize = 0;
  char * data = NULL;
  int id_size = 0;
  int name_size = 0;
  int sur_size = 0;
  int city_size = 0;
  BF_Block * block = NULL;
  BF_Block_Init(&block);

  if(BF_GetBlockCounter(fileDesc, &blocks) != BF_OK)
    return SR_ERROR;

  if(blocks == 1){
    if(BF_AllocateBlock(fileDesc, block) != BF_OK)
      return SR_ERROR;
    data = BF_Block_GetData(block);
    offset = init_block(data, &record);
  }
  else {
    if(BF_GetBlock(fileDesc, blocks-1, block) != BF_OK)
      return SR_ERROR;
    data = BF_Block_GetData(block);
    //get how many records are in the block
    memmove(&recs, data, sizeof(int));
    offset = sizeof(int);
    //get sizes of Record
    memmove(&id_size, data+offset, sizeof(int));
    offset += sizeof(int);
    memmove(&name_size, data+offset, sizeof(int));
    offset += sizeof(int);
    memmove(&sur_size, data+offset, sizeof(int));
    offset += sizeof(int);
    memmove(&city_size, data+offset, sizeof(int));
    offset += sizeof(int);

    //check if there is available space left
    recsize = id_size + name_size + sur_size + city_size;
    offset += recs*(id_size + name_size + sur_size + city_size);
    avail_space = BF_BLOCK_SIZE - offset;

    //allocate new block as above
    if(avail_space - recsize< 0){
      //unpin previous block
      BF_UnpinBlock(block);
      if(BF_AllocateBlock(fileDesc, block) != BF_OK)
        return SR_ERROR;
      data = BF_Block_GetData(block);
      offset = init_block(data, &record);
      recs = 0;
    }
  }

  //copy Record
  memmove(data+offset, &(record.id), sizeof(int));
  offset += sizeof(int);
  memmove(data+offset, record.name, sizeof(record.name));
  offset += sizeof(record.name);
  memmove(data+offset, record.surname, sizeof(record.surname));
  offset += sizeof(record.surname);
  memmove(data+offset, record.city, sizeof(record.city));
  //update record counter and set block to dirty
  recs++;
  memmove(data, &recs, sizeof(int));

  BF_Block_SetDirty(block);
  if(BF_UnpinBlock(block) != BF_OK)
    return SR_ERROR;
  BF_Block_Destroy(&block);
  return SR_OK;
}

SR_ErrorCode SR_SortedFile(
  const char* input_filename,
  const char* output_filename,
  int fieldNo,
  int bufferSize
) {
  if(bufferSize<3 || bufferSize>BF_BUFFER_SIZE)
    return BAD_bufferSize;
  int fileDesc;
  SR_OpenFile(input_filename,&fileDesc);
/******************************************************************************
***********sort the file into runs and save them in a temporary file***********
******************************************************************************/
  //creating a copy of the input_file so it remains unchanged
  SR_CreateFile("tempSortFile");
  int tempFileDesc;
  SR_OpenFile("tempSortFile",&tempFileDesc);
  CopyFile(tempSortFile,fileDesc);

  /*initialize pinnedBlocks:  This is where we keep the BF_Block* of the blocks
                              that are currently pinned.*/
  BF_Block* pinnedBlocks[bufferSize];
  for(int i=0; i<bufferSize; i++)
      BF_Block_Init(&pinnedBlocks[i]);

  //initialize some counters
  int BlockCount, iteratedBlocks = 0;
  BF_GetBlockCounter(fileDesc,&BlockCount);
  iteratedBlocks++; //skip the metadata block
  int lastRunSize = BlockCount%bufferSize;
  Index low, high;
  low.blockIndex = 0;
  low.recordIndex = 0;
  high.blockIndex = bufferSize - 1;
  high.recordIndex = (BF_BLOCK_SIZE - BLOCKBASEOFFSET) / SIZEOFRECORD - 1;

  //get,sort and store the runs one by one
  while(iteratedBlocks < BlockCount-lastRunSize){
      //get the run to the buffers
      for(int i=0; i<bufferSize; i++){
        BF_GetBlock(tempFileDesc,iteratedBlocks,pinnedBlocks[i]);
        iteratedBlocks++;
      }
      //sort the run
      QuickSortRun(pinnedBlocks,bufferSize,fieldNo, low, high);
      //store the sorted run back to the temp_file
      for(int i=0; i<bufferSize; i++){
        BF_UnpinBlock(pinnedBlocks[i]);
        pinnedBlocks[i] = NULL;
      }
  }
  //***do the last run as well***
  //get the last run to the buffers
  for(int i=0; i<lastRunSize; i++){
    BF_GetBlock(fileDesc,iteratedBlocks,pinnedBlocks[i]);
    iteratedBlocks++;
  }
  //sort the last run
  //EDW TO HIGH EINAI ALLO E
  QuickSortRun(pinnedBlocks,bufferSize,fieldNo, low, high);
  //store it
  for(int i=0; i<lastRunSize; i++){
    BF_UnpinBlock(pinnedBlocks[i]);
    pinnedBlocks[i] = NULL;
  }
/******************************************************************************
**************merge the runs and store them in the output_filename*************
******************************************************************************/

  return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  // Your code goes here

  return SR_OK;
}
