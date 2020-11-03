#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#define SIZE_NUM_STR 20
#define SIZE_NAME_FILE 50

typedef struct buffer{
  int* mem;
  int n;
  int size;
}Buffer;

Buffer* create_buffer(int size){
  Buffer* bf = (Buffer*) malloc(sizeof(Buffer));
  bf->n = 0;
  bf->size = size;
  bf->mem = (int*) calloc(size, sizeof(int));
  memset(bf->mem,0, bf->size*sizeof(int));
  return bf;
}

int full_buffer(Buffer* bf){
  return (bf->n==bf->size);
}

void add_ext(char* s, char* ext){
  strcat(s, ext);
}

int read_mem_rec(Buffer **mem_buffer, Buffer** frz_buffer, FILE*  bin_file, int size){
  //Saving the first 'size'  elements in memory and creating frozen buffer
  int read_nums = 0;
  int key;

  (*mem_buffer) = create_buffer(size);
  while((!full_buffer(*mem_buffer)) && (fread(&key, sizeof(int), 1, bin_file))!=0){
      int pos = (*mem_buffer)->n;
      (*mem_buffer)->mem[pos] = key;
      (*mem_buffer)->n++;
  }

  read_nums = (*mem_buffer)->n;

  if(read_nums == 0){
    free(mem_buffer);
  }else{
    *frz_buffer = create_buffer(size);
  }

  return read_nums;
}

void generate_name_file_part(char* name_file_part, char* file_name, int num_part){
  char num_str[SIZE_NUM_STR];
  sprintf(num_str,"_part_%d",num_part);
  strcpy(name_file_part, file_name);
  strcat(name_file_part, num_str);
}

void generate_names_file_name_part(char* names_file_name_part, char* file_name, char* ext){
  strcpy(names_file_name_part, file_name);
  strcat(names_file_name_part, "_names_part");
  add_ext(names_file_name_part, ext);
}

void generate_output_file_name(char* output_file_name, char* file_name){
  strcpy(output_file_name, file_name);
  strcat(output_file_name,"_MERGE");
}

FILE* open_partition(char* name_file_part, int num_part){
  FILE* file_part = fopen(name_file_part, "wb");
  if(!file_part){
    printf("Error creating partition %d", num_part);
    exit(1);
  }
  return file_part;
}
void thaw_frozen_buffers(Buffer* mem_buffer, Buffer* frz_buffer){
  frz_buffer->n= 0;
  memset(frz_buffer->mem, 0, frz_buffer->size*sizeof(int));
}

void print_buffer(Buffer* bf){
  for(int i =0;i<bf->size-1;i++){
    if(bf->mem[i] == INT_MAX){
      printf("INF ");
    }else{
      printf("%3d ", bf->mem[i]);
    }
    }

    if(bf->mem[bf->size-1] == INT_MAX){
      printf("INF\n");
    }else{
      printf("%3d\n", bf->mem[bf->size-1]);
    }

}

int find_smaller(Buffer* mem_buffer, Buffer* frz_buffer, int* smallest_key, int* ind_smallest_key){
  if((mem_buffer->n==0) || (frz_buffer->n == frz_buffer->size)) return 0;

  int i =0;
  *smallest_key = INT_MAX;
  *ind_smallest_key = -1;

  for(i=0;i<mem_buffer->n;i++){
    if(frz_buffer->mem[i] == 0){
      if(*smallest_key>mem_buffer->mem[i]){
         *smallest_key = mem_buffer->mem[i];
         *ind_smallest_key = i;
      }
    }
  }

  return(*ind_smallest_key!=-1);
}
void generate_partition_sub_sel(char* file_name, int mem_size){
  FILE* bin_file = fopen(file_name, "rb");
  char ext[5] = ".txt";
  if(!bin_file) return;
  Buffer* mem_buffer, *frz_buffer;

  if(read_mem_rec(&mem_buffer, &frz_buffer, bin_file, mem_size) == 0) return;
  printf("\n\nSize %d allocated memory buffer\n\n\n",mem_size);

  //Open files for processing
  char name_file_part[SIZE_NAME_FILE];
  char names_file_name_part[SIZE_NAME_FILE];
  int num_part = 1;

  generate_name_file_part(name_file_part, file_name, num_part);
  FILE* file_part = open_partition(name_file_part, num_part);

  generate_names_file_name_part(names_file_name_part, file_name, ext);
  FILE* names_file_part = fopen(names_file_name_part, "w"); //It's a txt File because it will save the name of the partitions to be created

  fprintf(names_file_part, "%s\n", name_file_part);

  //Partitions creator

  print_buffer(mem_buffer);

  print_buffer(frz_buffer);

  int end_of_file = 0;
  int smallest_key, ind_smallest_key;
  int processing = 1;

  printf("\nOpening partition %d\n\n", num_part);

  while(processing){
    if(find_smaller(mem_buffer,frz_buffer,&smallest_key,&ind_smallest_key)){

        printf("Written key %d\n", smallest_key);
        fwrite(&smallest_key, sizeof(int),1,file_part); //Saves the key to the current partition(the one currently open)

        int new_key;
        if(end_of_file || (fread(&new_key, sizeof(int), 1, bin_file) == 0)){//zero == "EOF"
          end_of_file = 1;
          mem_buffer->mem[ind_smallest_key] = INT_MAX;
        }else{
          mem_buffer->mem[ind_smallest_key]=new_key;
          if(new_key<smallest_key){ //Need to freze the new key read
              frz_buffer->mem[ind_smallest_key] = 1;
              frz_buffer->n++;

          }
        }

    }else{
      //All frozen or there's no more to save
        printf("\nClosing partition %d\n\n", num_part);
        fclose(file_part);

        if(frz_buffer->n>0){//if there are still frozen, thaw and start new partition
            num_part++;
            printf("\nOpen partition %d\n\n", num_part);
            generate_name_file_part(name_file_part, file_name, num_part);
            file_part = open_partition(name_file_part, num_part);
            thaw_frozen_buffers(mem_buffer, frz_buffer);
            fprintf(names_file_part, "%s\n", name_file_part);

        }else{
            processing = 0;
        }
    }

    print_buffer(mem_buffer);
    print_buffer(frz_buffer);

  }
  fclose(bin_file);
  fclose(file_part);
  fclose(names_file_part);
}
void print_keys(int num_part, int* keys){
  for(int i =0;i<num_part;i++){
    if(keys[i]!=INT_MAX){
      printf("%3d ", keys[i]);
    }else{
      printf("INF ");
    }
  }
}
void read_keys(FILE** file_part, int num_part, int* keys){
  for(int i =0;i<num_part;i++){
    fread(&keys[i], sizeof(int), 1, file_part[i]);
  }
}

int search_smallest(int* keys, int num_part, int* ind_smallest, int* smallest_key){
  *ind_smallest = -1;
  *smallest_key = INT_MAX;

  for(int i = 0;i<num_part;i++){
    if(keys[i]<*smallest_key){
      *ind_smallest=i;
      *smallest_key=keys[i];
    }
  }

  return(*ind_smallest!= -1);
}
void update_keys(FILE* file, int* key){
  int new_key;
  if(fread(&new_key, sizeof(int), 1, file)){
    *key = new_key;
  }else{
    *key = INT_MAX;
  }
}

void merge(char* file_name){
  FILE** file_part; //Array of file pointers
  FILE* file_out;
  int num_part = 0;
  char file_name_part[SIZE_NAME_FILE];
  char file_name_out[SIZE_NAME_FILE];
  char file_names_name_part[SIZE_NAME_FILE];

  char ext[5] = ".txt";

  printf("\n\n Merge\n\n");
  //Determine the number of partitions to be opened
  generate_names_file_name_part(file_names_name_part, file_name, ext);
  FILE* names_file_part = fopen(file_names_name_part, "r");
  while(fscanf(names_file_part, "%s", file_name_part)!=EOF){//Couns the total partitions
      num_part++;
  }
  fclose(names_file_part);

  //Open files from all partitions
  file_part = (FILE**) malloc(num_part*sizeof(FILE*));
  names_file_part = fopen(file_names_name_part, "r"); //Create a descriptor for each partition
  for(int i = 0;i<num_part;i++){
    fscanf(names_file_part, "%s", file_name_part);
    file_part[i] = fopen(file_name_part, "rb");
  }

  //Read the initial keys
  int* keys = (int*) malloc(num_part*sizeof(int));

  read_keys(file_part, num_part, keys);

  //Open the output file with the result of the merge for writing
  generate_output_file_name(file_name_out, file_name);
  file_out = fopen(file_name_out, "wb");

  //Merge the partitions

  int ind_smallest_key, smallest_key;

  while(search_smallest(keys, num_part, &ind_smallest_key, &smallest_key)){
    print_keys(num_part, keys);
    fwrite(&smallest_key, sizeof(int), 1, file_out);
    printf("  |  saved key: %3d\n", smallest_key);
    update_keys(file_part[ind_smallest_key], &keys[ind_smallest_key]);
  }

  //Free key array
  free(keys);

  //Close partitions
  for(int i =0; i<num_part;i++){
    fclose(file_part[i]);
  }
  //Close output file

  fclose(file_out);
  fclose(names_file_part);

}
int main()
{
    char file_name[SIZE_NAME_FILE];
    printf("Enter the file name to sort: ");
    scanf("%s",file_name);
    generate_partition_sub_sel(file_name, 6);
    merge(file_name);

    return 0;
}
