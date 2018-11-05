#include "a2_lib.h"

typedef struct Container {
	char* key; 
	char* value[CONTAINER_SIZE]; //each hashkey has 16 possible subcontainers that may contain other values
} Container;

int kv_store_create(char *kv_store_name){

	int fd = shm_open(kv_store_name, O_CREAT|O_EXCL, S_IRWXU);

	if(fd==-1){
		perror("shm failed");
		return -1;
	}

	int trc=ftruncate(fd, __KEY_VALUE_STORE_SIZE__);

	if(trc==-1){
		perror("truncate failed");
		return -1;
	}

	char* address = mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	memset(address, '\0', __KEY_VALUE_STORE_SIZE__);
	
	close(fd);
	munmap(address, __KEY_VALUE_STORE_SIZE__);
	
	return 0;
}

int kv_store_write(char *key, char *value) {
	
	sem_wait(&writer_lock);
	
	if(sizeof(value)> VALUE_SIZE){
		value=strndup(value, VALUE_SIZE);
	}

	if(sizeof(key)> KEY_SIZE){
		key=strndup(key, KEY_SIZE);
	}

	int fd = shm_open(__KV_STORE_NAME__, O_RDWR, 0);

	if(fd==-1){
		perror("shm failed");
		return -1;
	}

	unsigned long hashKey = generate_hash(key);
	int index = hashKey%CONTAINER_SIZE;
	int offset = index * (__KEY_VALUE_STORE_SIZE__/CONTAINER_SIZE);

	struct Container *container = mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	
	while(true){
		if(strcmp(container[index].key, key)==0){ //find an open slot
			for(int i=0; i<CONTAINER_SIZE; i++){
				if(strcmp(container[index].value[i], '\0')==0){
					container[index].value[i]=value;
					container[index].key=key;
					break;
				}
			}
			container[index].value[0]=value; //if full clear out the first value.
			container[index].key=key;
		}
		else if(strncmp(container[index].key, '\0', 1)==0){
			container[index].key=key;
			container[index].value[0]=value;
			break;
		}
		else {
			index+=1;
			index%=CONTAINER_SIZE; //try next pod
		}
	}

	close(fd);
	munmap(container, __KEY_VALUE_STORE_SIZE__);
	free(container);

	sem_post(&writer_lock); //unlock semaphore
	
}

char *kv_store_read(char *key){
	// implement your create method code here

	unsigned long hashKey = generate_hash(key);

	sem_wait(&reader_lock);
	read_count++;
	if(read_count==1){
		sem_wait(&writer_lock);
	}
	sem_post(&reader_lock);

	//readstuff
	int fd = shm_open(__KV_STORE_NAME__, O_RDWR, 0);
	struct Container *container = mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_READ, MAP_SHARED, fd, 0);
	int index = hashKey%CONTAINER_SIZE;
	char* toReturn= calloc(1, VALUE_SIZE);
	for(int i=0; i<CONTAINER_SIZE; i++){
		if(strcmp(container[index].key, key)==0){
			toReturn=container[index].value[i]; //returns first discovered value for particular key
			break;
		}
	}
	close(fd);
	munmap(container,__KEY_VALUE_STORE_SIZE__);
	free(container);
	return toReturn;


	return NULL; 
}

char **kv_store_read_all(char *key){
	// implement your create method code here
	return NULL;
}

void kv_delete_db(){
	
	sem_unlink(__KV_READERS_SEMAPHORE__);
	sem_unlink(__KV_WRITERS_SEMAPHORE__);

	kill_shared_mem();

}

int kv_create(){
	sem_t writer_lock = sem_open(__KV_WRITERS_SEMAPHORE__, O_CREAT | O_EXCL, 0644, 1);
    sem_t reader_lock = sem_open(__KV_READERS_SEMAPHORE__, O_CREAT | O_EXCL, 0644, 1);
}

int main() {
	return EXIT_SUCCESS;
}
