#include<stdio.h>
#include<stdint.h>
#include<stdlib.h>

struct Node{
    void *data;
    struct Node *next;
};

struct Node *create_new_node(void* data) {
    struct Node *new_node = (struct Node*) malloc(sizeof(struct Node));
    new_node->data = data;
    new_node->next = NULL;

    return new_node;
}

void print_all_nodes(struct Node *header){
    struct Node *current_node = header;
    uint32_t counter = 0;
    while (current_node != NULL)
    {
        if(counter == 0){
            printf("Data0 is %d\n", *(int *)current_node->data);
        }else if(counter == 1){
            printf("Data1 is %.2f\n", *(float *)current_node->data);
        }else{
            printf("Data2 is %s\n", (char *)current_node->data);
        }
        counter++;
        current_node = current_node->next;
    }
}

int main()
{
    int data1 = 24;
    float data2 = 35.67;
    char data3[] = "Hello";
    struct Node *header = create_new_node(&data1);
    header->next = create_new_node(&data2);
    header->next->next = create_new_node(data3);

    print_all_nodes(header);

    return 0;
}