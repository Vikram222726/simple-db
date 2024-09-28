#include<stdio.h>
#include<unistd.h>

int main(){
    char buffer[100];
    ssize_t bytes_read;

    bytes_read = read(0, buffer, sizeof(buffer));
    if(bytes_read < 0) {
        printf("Error reading buffer size");
    }else{
        printf("Bytes read from buffer is %zd \n", bytes_read);
    }

    return 0;
}