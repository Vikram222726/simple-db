#include <stdio.h>
#include <stddef.h>

int main() {
    int arr[10];
    size_t size_of_arr = sizeof(arr);
    printf("Size of arr is %zu bytes\n", size_of_arr);

    return 0;
}