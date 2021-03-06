#include <math.h>
#include <stdlib.h>
#include <string.h>
#include "sort_file.h"
#include "bf.h"
#include "HelperFunctions.h"
#include "Run.h"

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
  if(strcmp(message,"Sort")){
    fprintf(stderr, "File to be opened is not a sort file!\n");
    return SR_ERROR;
  }
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


int SR_DestroyFile(char *fileName) {
  BF_Block *tmpBlock;
  BF_Block_Init(&tmpBlock);

  int fileDesc;
  char *data = NULL;


  BF_OpenFile(fileName, &fileDesc);

  BF_GetBlock(fileDesc, 0, tmpBlock);//Getting the first block
  data = BF_Block_GetData(tmpBlock);//and its data

  if (data == NULL || strcmp(data, "Sort"))//to check if this new opened file is a sort file
  {
    BF_UnpinBlock(tmpBlock);
    BF_Block_Destroy(&tmpBlock);
    BF_CloseFile(fileDesc);
    printf("File: %s to destroy is not a sort tree file. Exiting..\n", fileName);
    exit(-1);
  }

  BF_UnpinBlock(tmpBlock);
  BF_Block_Destroy(&tmpBlock);
  BF_CloseFile(fileDesc);

  remove(fileName);
  return 1;
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

SR_ErrorCode SR_PrintAllEntries(int fileDesc);

SR_ErrorCode SR_SortedFile(
  const char* input_filename,
  const char* output_filename,
  int fieldNo,
  int bufferSize
) {
  if(bufferSize<3 || bufferSize>BF_BUFFER_SIZE)
    return BAD_bufferSize;
/******************************************************************************
***********sort the file into runs and save them in a temporary file***********
******************************************************************************/
  //creating a copy of the input_file so it remains unchanged
  int fileDesc,tempDesc;
  SR_OpenFile(input_filename,&fileDesc);
  SR_CreateFile("tempSortFile");
  SR_OpenFile("tempSortFile", &tempDesc);
  CopyFile(fileDesc, tempDesc);
  SR_CloseFile(fileDesc);

  /*initialize pinnedBlocks:  This is where we keep the BF_Block* of the blocks
                              that are currently pinned.*/
  BF_Block** pinnedBlocks = malloc(bufferSize*sizeof(BF_Block*));

  //initialize some counters
  int BlockCount, iteratedBlocks = 0;
  BF_GetBlockCounter(tempDesc, &BlockCount);
  iteratedBlocks++; //skip the metadata block
  int lastRunSize = (BlockCount-1)%bufferSize;

  //get,sort and store the runs one by one
  while(iteratedBlocks <= BlockCount-lastRunSize -bufferSize){
      //printf("ItB %d Bc %d lrs %d buf %d\n" ,iteratedBlocks, BlockCount, lastRunSize, bufferSize);
      //get the run to the buffers
      for(int i=0; i<bufferSize; i++)
          BF_Block_Init(&pinnedBlocks[i]);
      for(int i=0; i<bufferSize; i++){
        BF_GetBlock(tempDesc, iteratedBlocks, pinnedBlocks[i]);
        iteratedBlocks++;
      }
      //sort the run
      quickSort(pinnedBlocks, bufferSize, fieldNo, 0, lastRunSize);
      //store the sorted run back to the temp_file
      for(int i=0; i<bufferSize; i++){
        BF_Block_SetDirty(pinnedBlocks[i]);
        BF_UnpinBlock(pinnedBlocks[i]);
        BF_Block_Destroy(&pinnedBlocks[i]);
        //pinnedBlocks[i] = NULL;
      }
  }
  for(int i=0; i<lastRunSize; i++)
      BF_Block_Init(&pinnedBlocks[i]);
  //***do the last run as well***
  //get the last run to the buffers
  for(int i=0; i<lastRunSize; i++){
    BF_GetBlock(tempDesc,iteratedBlocks,pinnedBlocks[i]);
    iteratedBlocks++;
  }
  //sort the last run
  if(lastRunSize)
    quickSort(pinnedBlocks, bufferSize, fieldNo, 1, lastRunSize);
  //store it
  for(int i=0; i<lastRunSize; i++){
    BF_Block_SetDirty(pinnedBlocks[i]);
    BF_UnpinBlock(pinnedBlocks[i]);
    BF_Block_Destroy(&pinnedBlocks[i]);
    //pinnedBlocks[i] = NULL;
  }
  free(pinnedBlocks);
/******************************************************************************
**************merge the runs and store them in the output_filename*************
******************************************************************************/
  int run_size = bufferSize;
  BlockCount--; //minus the metadata block
  //quick mafs to determine how many iterations we'll need to sort the file
  int m = ceil( (double)BlockCount/(double)bufferSize );
  unsigned int iterations = ceil( (double)log(m)/(double)log(bufferSize-1) );

  int in_file = tempDesc; //this is where we get runs from
  char in_filename[32] = "tempSortFile";
  int out_file;         //this is where we store merged runs to
  char out_filename[32];
  if(iterations > 1)
    strcpy(out_filename,"outFile1");
  else
    strcpy(out_filename,output_filename);
  SR_CreateFile(out_filename);
  SR_OpenFile(out_filename,&out_file);
  /*initialize pinnedRuns:  This is where we keep the Runs
                            that are currently pinned.*/
  Run** pinnedRuns = malloc((bufferSize-1)*sizeof(Run *));

  /*Sort the whole file into bigger runs.
   Repeat until the whole file is a sorted run,
   but hold on for the last iteration,
   i want to save it specifically to out_file*/
  for(int iteration=1; iteration<=iterations; iteration++){
    int current_block_id = 1;
    int num_of_unmerged_blocks = BlockCount;
    //total number of runs in the file
    int runs_in_file = (int)ceil((double)BlockCount/(double)run_size);
    //number of run-groups
    int groups_in_file = (int)ceil((double)runs_in_file/(double)(bufferSize-1));

    /*load,sort,store all the groups one by one*/
    for(int g=0; g<groups_in_file; g++){
      /*load group to the buffers*/
      int group_size = PinGroup(pinnedRuns,in_file,&current_block_id,run_size,
                            &num_of_unmerged_blocks,lastRunSize,bufferSize);
      //The buffers are full now. Just one block is left for the output.
      //Lets sort and store this group of runs
      SortAndStoreRuns(pinnedRuns,group_size,fieldNo,out_file);
      //clean the pinnedRuns array
      UnpinGroup(pinnedRuns,group_size);
    }

    /*Swap files and prepare for next iteration*/
    SR_CloseFile(in_file);
    SR_DestroyFile(in_filename);
    in_file = out_file;
    strcpy(in_filename,out_filename);
    /*The last iteration must be written to the out_file*/
    if(iteration+1 == iterations){ //if the next iteration is the last iteration
      SR_CreateFile(output_filename);
      SR_OpenFile(output_filename,&out_file);
      strcpy(out_filename,output_filename);
    }
    //create a new out_file named "outFile*here_goes_iteration_number*"
    else if(iteration != iterations){ //if its the last iteration we wont need an outfile
      strcpy(out_filename,"outFile");
      char file_serial_num[10];
      snprintf(file_serial_num, 10, "%d", iteration+1);//iteration as a string
      strcat(out_filename, file_serial_num);  //example "outFile16"
      SR_CreateFile(out_filename);
      SR_OpenFile(out_filename, &out_file);
    }
    //runs have been merged in groups, the new run is a whole group
    run_size = run_size*(bufferSize-1);
    lastRunSize = BlockCount%run_size;
  }
  //The whole file is sorted now
  SR_CloseFile(out_file);
  free(pinnedRuns);
  return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  // Your code goes here
  int blocks = 0, offset = 0, recs = 0, id;;
  int rsize = sizeof(Record);
  char * data = NULL;
  BF_Block * block = NULL;
  int id_size = 0;
  int name_size = 0;
  int sur_size = 0;
  int city_size = 0;
  BF_ErrorCode err;
  BF_Block_Init(&block);

  if(err = BF_GetBlockCounter(fileDesc, &blocks) != BF_OK)
    return SR_ERROR;

  //for all blocks
  for(int i=1; i<blocks; i++){
    if(BF_GetBlock(fileDesc, i, block) != BF_OK)
      return SR_ERROR;
    offset = 0;
    //get number of recs in the block
    data = BF_Block_GetData(block);
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

    //for each block print the entries
    for(int j=0; j< recs; j++){
      //print id
      memmove(&id, data+offset, id_size);
      printf("%d ", id);
      offset += id_size;
      //print name
      printf("%s ", data+offset);
      offset += name_size;
      //print surname
      printf("%s ", data+offset);
      offset += sur_size;
      //print city
      printf("%s\n", data+offset);
      offset += city_size;
    }
    BF_UnpinBlock(block);
    printf("\n\n");
  }
  BF_Block_Destroy(&block);

  return SR_OK;
}
