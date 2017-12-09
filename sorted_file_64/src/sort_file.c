#include <string.h>
#include "sort_file.h"
#include "bf.h"

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

SR_ErrorCode SR_InsertEntry(int fileDesc,	Record record) {
  // Your code goes here

  return SR_OK;
}

SR_ErrorCode SR_SortedFile(
  const char* input_filename,
  const char* output_filename,
  int fieldNo,
  int bufferSize
) {
  // Your code goes here

  return SR_OK;
}

SR_ErrorCode SR_PrintAllEntries(int fileDesc) {
  // Your code goes here

  return SR_OK;
}
