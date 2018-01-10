
void swap(int *blockArray, int x, int y){
  int temp = blockArray[x];
  blockArray[x] = blockArray[y];
  blockArray[y] = temp;
}

void heapify(int *blockArray, int n, int i){
  int max = i, l = (max << 1) +1, r = (max + 1) << 1;

  if(l < n && blockArray[l] > blockArray[max])
    max = l;

  if(r < n && blockArray[r] > blockArray[max])
    max = r;

  if(max != i){
    swap(blockArray, i, max);
    heapify(blockArray, n, max);
  }
}

void makeheap(){

}hea
