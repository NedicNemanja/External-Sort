#include <string.h>
#include <math.h>
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
  //SR_CreateFile("tempSortFile");
  int tempDesc;
  //SR_OpenFile("tempSortFile",&tempDesc);
  //CopyFile(tempDesc,fileDesc);
  //TO COPY GINETE LOW LEVEL BYTE PER BYTE XWRIS SR CREATE FILE KLP NA DEN LITOURGISEI TO KANOUME HIGH LEVEL
  tempDesc = copyFile(input_filename, "tempSortFile");
  //EPISIS O TEMPDESC EPISTREFETE ANOIXTOS, NA KLEISTEI KAPOU PIO KATW

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

  /*??????????????????????????????????????????????????????????????????????????
  ???????????????????????????????????????????????????????????????
  ??????????????????????????????????????????????????*/
  
  //????????????????????????????????????????????????????????????????????????

  //get,sort and store the runs one by one
  while(iteratedBlocks < BlockCount-lastRunSize){
      //get the run to the buffers
      for(int i=0; i<bufferSize; i++){
        BF_GetBlock(tempDesc,iteratedBlocks,pinnedBlocks[i]);
        iteratedBlocks++;
      }
      //sort the run
      quickSort(pinnedBlocks, bufferSize, fieldNo, 0, lastRunSize);
      //store the sorted run back to the temp_file
      for(int i=0; i<bufferSize; i++){
        BF_Block_SetDirty(pinnedBlocks[i]);
        BF_UnpinBlock(pinnedBlocks[i]);
        pinnedBlocks[i] = NULL;
      }
  }
  //***do the last run as well***
  //MPOREI TO LAST RUN NA DIERITE AKRIVWS EDW KAI NA MIN IPARXEI, NA VALOUME IF AN IPARXEI NA KANEI TA APO KATW?
  //get the last run to the buffers
  for(int i=0; i<lastRunSize; i++){
    BF_GetBlock(tempDesc,iteratedBlocks,pinnedBlocks[i]);
    iteratedBlocks++;
  }
  //sort the last run
  //EDW TO HIGH EINAI ALLO E
  quickSort(pinnedBlocks, bufferSize, fieldNo, 1, lastRunSize);
  //store it
  for(int i=0; i<lastRunSize; i++){
    BF_Block_SetDirty(pinnedBlocks[i]);
    BF_UnpinBlock(pinnedBlocks[i]);
    pinnedBlocks[i] = NULL;
  }
  //destroy pinnedBlocks to free the buffers
  for(int i=0; i<bufferSize; i++){
    BF_Block_Destroy(&pinnedBlocks[i]);
  }
/******************************************************************************
**************merge the runs and store them in the output_filename*************
******************************************************************************/
  int run_size = bufferSize;
  BlockCount--; //minus the metadata block
  int in_file=tempDesc; //this is where we get runs from
  int out_file;         //this is where we store runs to
  SR_CreateFile("tempFile");
  SR_OpenFile("tempFile",&out_file);
  int m = (int)ceil( (double)BlockCount/(double)bufferSize );
  int iterations = (int)(log(bufferSize-1)/log(m));

  /*initialize pinnedRuns:  This is where we keep the Runs
                            that are currently pinned.*/
  Run* pinnedRuns[bufferSize-1];

  int iteration=1;
  /*repeat until the whole file is sorted,
  but hold on for the last iteration,i want to save it specifically to out_file*/
  while(iteration<iterations){
    int current_block_id = 1;
    int num_of_unmerged_blocks = BlockCount;
    int runs_in_file = (int)ceil((double)BlockCount/(double)run_size);
    int groups_in_file = (int)ceil((double)runs_in_file/(double)(bufferSize-1));

    /*group the runs in (bufferSize-1) groups*
    exception: the last run of the last group*/
    for(int g=0; g<groups_in_file-1; g++){
      /*for every run of the group*/
      for(int buffer_index=0; buffer_index<=bufferSize-1; buffer_index++){
        //pin a run and dedicate a buffer to it
        pinnedRuns[buffer_index] = Run_init(in_file,current_block_id,run_size);
        //look at the next run
        num_of_unmerged_blocks -= run_size;
        current_block_id += run_size;
      }
      //The buffers are full now. Just one block is left for the output.
      //Lets sort and store this group of runs
      HeapSortRun(pinnedRuns,bufferSize-1,fieldNo,out_file);
    }
    /*Exception: last group with the last run*/
    for(int buffer_index=0; buffer_index<=bufferSize-1; buffer_index++){
      //pin a run and dedicate a buffer to it
      pinnedRuns[buffer_index] = Run_init(in_file,current_block_id,run_size);
      //look at the next run
      num_of_unmerged_blocks -= run_size;
      //if this is the last run
      if(num_of_unmerged_blocks == lastRunSize){
        current_block_id += lastRunSize;
        pinnedRuns[buffer_index] = Run_init(in_file,current_block_id,lastRunSize);
        break;
      }
      else
        current_block_id += run_size;
    }
    //The buffers are full now. Just one block is left for the output.
    //Lets sort and store the LAST group of runs for this iteration
    HeapSortRun(pinnedRuns,bufferSize-1,fieldNo,out_file);

    /*Flush and prepare for next iteration*/
    //pinnedRunsDestroy();

    //in_file destroy
    in_file = out_file;
    SR_CreateFile("tempFile");
    SR_OpenFile("tempFile",&out_file);

    run_size = run_size*(bufferSize-1);
    iteration++;
  }

  return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  // Your code goes here

  return SR_OK;
}
