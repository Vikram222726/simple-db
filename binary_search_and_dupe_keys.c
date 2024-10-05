#include<stdbool.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<stdint.h>
#include<errno.h>
#include<fcntl.h>
#include<unistd.h>

#define MAX_USERNAME_CHAR 32
#define MAX_EMAIL_CHAR 255
#define MAX_TABLE_PAGES 100
#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)

typedef enum
{
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED
} MetaCommandResult;

typedef enum
{
    NODE_LEAF,
    NODE_INTERNAL
} NodeType;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_INVALID_ID,
    PREPARE_USERNAME_TOO_LONG,
    PREPARE_EMAIL_TOO_LONG,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum
{
    STATEMENT_SELECT,
    STATEMENT_INSERT
} StatementType;

typedef enum
{
    EXECUTE_SUCCESS,
    EXECUTE_FAILED,
    EXECUTE_DUPLICATE_KEY,
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct {
    void *pages[MAX_TABLE_PAGES];
    uint32_t file_length;
    uint32_t num_pages;
    int file_descriptor;

} Pager;

typedef struct
{
    uint32_t rows_count;
    Pager *pager;
    uint32_t root_page_num;
} Table;

typedef struct {
    uint32_t id;
    // Extra 1 char will be used for assigning NULL character to a string in C.
    char username[MAX_USERNAME_CHAR + 1];
    char email[MAX_EMAIL_CHAR + 1];
} Row;

typedef struct {
    char* buffer;
    size_t buffer_size;
    ssize_t text_size;
} InputBuffer;

typedef struct {
    StatementType type;
    Row row_data;
} Statement;

typedef struct{
    Table *table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * MAX_TABLE_PAGES;

// Common Node header format => NODE_TYPE, IS_ROOT_NODE, NODE_PARENT_POINTER
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_NODE_ROOT_SIZE = sizeof(uint8_t);
const uint32_t IS_NODE_ROOT_OFFSET = NODE_TYPE_OFFSET + NODE_TYPE_SIZE;
const uint32_t NODE_PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t NODE_PARENT_POINTER_OFFSET = IS_NODE_ROOT_OFFSET + IS_NODE_ROOT_SIZE;
const uint32_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_NODE_ROOT_SIZE + NODE_PARENT_POINTER_SIZE;

// Leaf Node Header format => COMMON_NODE_HEADER, LEAF_CELLS_COUNT
const uint32_t LEAF_NODE_CELLS_COUNT_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_CELLS_COUNT_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_CELLS_COUNT_SIZE;

// Leaf Node Body format => Key, Value
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_CELL_SPACE = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_CELL_SPACE / LEAF_NODE_CELL_SIZE;

NodeType get_node_type(void* node){
    uint8_t type = *((uint8_t *)(node + NODE_TYPE_OFFSET));
    return (NodeType)type;
}

void set_node_type(void* node, NodeType type){
    uint8_t node_type = type;
    *((uint8_t *)(node + NODE_TYPE_OFFSET)) = node_type;
}

// Accessing Leaf Node fields..
uint32_t* leaf_node_num_cells(void* node){
    return node + LEAF_NODE_CELLS_COUNT_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num){
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

uint32_t* leaf_node_key(void* node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num);
}

void* leaf_node_value(void* node, uint32_t cell_num){
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void initialize_leaf_node(void* node){
    set_node_type(node, NODE_LEAF);
    uint32_t *num_cells_node = leaf_node_num_cells(node);
    *num_cells_node = 0;
}

void *get_page(Pager *pager, uint32_t page_num){
    if(page_num > MAX_TABLE_PAGES){
        printf("Error: page_num out of bound\n");
        exit(EXIT_FAILURE);
    }

    if(pager->pages[page_num] == NULL){
        void *page = (void *)malloc(PAGE_SIZE);

        uint32_t num_pages_file = pager->file_length / PAGE_SIZE;

        // If we have partial filled page stored in file, add that as well.
        if((pager->file_length) % PAGE_SIZE > 0){
            num_pages_file++;
        }

        if(page_num <= num_pages_file){
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if(bytes_read == -1){
                printf("Error: reading page from file on disk %d \n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;

        if(page_num >= pager->num_pages){
            pager->num_pages = page_num + 1;
        }
    }

    return pager->pages[page_num];
}

void serialize_row_data(Row* row_data, void* row_slot){
    memcpy(row_slot + ID_OFFSET, &(row_data->id), ID_SIZE);
    memcpy(row_slot + USERNAME_OFFSET, row_data->username, USERNAME_SIZE);
    memcpy(row_slot + EMAIL_OFFSET, row_data->email, EMAIL_SIZE);
}

void deserialize_row_data(Row* destination,void* source){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void leaf_node_insert(Cursor *cursor, uint32_t key, Row* row_data){
    void *node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells_page = *leaf_node_num_cells(node);

    if(num_cells_page >= LEAF_NODE_MAX_CELLS){
        printf("Leaf Node is completely full. Can't insert more cells in this Page!");
        exit(EXIT_FAILURE);
    }

    if((cursor->cell_num) < num_cells_page){
        for (uint32_t i = num_cells_page; i > (cursor->cell_num); i--){
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row_data(row_data, leaf_node_value(node, cursor->cell_num));
}

Pager* initialize_pager(char const* filename){
    int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

    if(fd == -1){
        printf("Error: Unable to open file \n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager *pager = (Pager *)malloc(sizeof(Pager));

    pager->file_length = file_length;
    pager->file_descriptor = fd;
    pager->num_pages = file_length / PAGE_SIZE;

    if (file_length % PAGE_SIZE != 0) {
     printf("Db file is not a whole number of pages. Corrupt file.\n");
     exit(EXIT_FAILURE);
    }


    for (uint32_t i = 0; i < MAX_TABLE_PAGES; i++){
        pager->pages[i] = NULL;
    }

    return pager;
}

Cursor* table_start(Table* table){
    Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
    new_cursor->table = table;
    new_cursor->page_num = table->root_page_num;
    new_cursor->cell_num = 0;

    void *node = get_page(new_cursor->table->pager, new_cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    new_cursor->end_of_table = (num_cells == 0);

    return new_cursor;
}

Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key_to_insert){
    void *node = get_page(table->pager, page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    Cursor *cursor = (Cursor *)malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;

    uint32_t lower_cell_index = 0;
    uint32_t upper_cell_index = num_cells;

    // Implement Binary Search to get required key index...
    while(lower_cell_index != upper_cell_index){
        uint32_t mid_cell_index = (lower_cell_index + upper_cell_index) / 2;

        uint32_t key_in_table = *leaf_node_key(node, mid_cell_index);

        // Condition when key already exists in table..
        if (key_in_table == key_to_insert)
        {
            cursor->cell_num = mid_cell_index;
            return cursor;
        }

        if(key_in_table > key_to_insert){
            upper_cell_index = mid_cell_index;
        }else{
            lower_cell_index = mid_cell_index + 1;
        }
    }

    cursor->cell_num = lower_cell_index;
    return cursor;
}

Cursor* table_find(Table* table,uint32_t key_to_insert){
    void *node = get_page(table->pager, table->root_page_num);

    NodeType node_type = get_node_type(node);
    if(node_type == NODE_LEAF){
        return leaf_node_find(table, table->root_page_num, key_to_insert);
    }else {
        printf("Implement logic for routing to leaf node via internal nodes");
        exit(EXIT_FAILURE);
    }
}

Table* open_db(const char* filename){
    Pager *pager = initialize_pager(filename);

    Table *new_table = (Table *)malloc(sizeof(Table));
    new_table->pager = pager;
    new_table->root_page_num = 0;

    if(pager->num_pages == 0){
        // New database file. Initialize page 0 as leaf node
        void *root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
    }

    return new_table;
}

void flush_page_to_disk(Pager *pager, int page_num){
    if (pager->pages[page_num] == NULL)
    {
        printf("Error: NULL pages cannot be flushed to disk\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if(offset == -1){
        printf("Error: seeking offset for page flush to disk\n");
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if(bytes_written == -1){
        printf("Error: writing pages from table to file on disk\n");
        exit(EXIT_FAILURE);
    }
}

void truncate_file_data_in_disk(Pager *pager){
    if(ftruncate(pager->file_descriptor, 0) == -1){
        printf("Error: Failed to truncate file data from disk \n");
        exit(EXIT_FAILURE);
    }
}

void db_close(Table *table){
    Pager *pager = table->pager;

    for (uint32_t i = 0; i < pager->num_pages;i++){
        if(pager->pages[i] == NULL){
            continue;
        }
        flush_page_to_disk(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    // truncate_file_data_in_disk(pager);
    ssize_t res = close(pager->file_descriptor);
    if(res == -1){
        printf("Error: while closing file descriptor!\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i = 0; i < MAX_TABLE_PAGES; i++){
        if(pager->pages[i] != NULL){
            free(pager->pages[i]);
            pager->pages[i] = NULL;
        }
    }

    free(pager);
    free(table);
}

InputBuffer* create_new_buffer(){
    InputBuffer* new_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));
    new_buffer->buffer = NULL;
    new_buffer->buffer_size = 0;
    new_buffer->text_size = 0;

    return new_buffer;
}

void print_prompt(){
    printf("simple_db > ");
}

void read_data_into_buffer(InputBuffer* buffer) {
    ssize_t bytes_read = getline(&(buffer->buffer), &(buffer->buffer_size), stdin);

    if(bytes_read <= 0){
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }
    buffer->text_size = bytes_read - 1;
    buffer->buffer[bytes_read - 1] = 0;
}

void close_input_buffer(InputBuffer* input_buffer){
    free(input_buffer->buffer);
    free(input_buffer);
}

void print_constants(){
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_CELL_SPACE);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

void print_btree(Table* table){
    void *node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("Number of Cells: %d \n", num_cells);

    for (uint32_t i = 0; i < num_cells; i++)
    {
        printf("Key: %d\n", *leaf_node_key(node, i));
    }
}

MetaCommandResult check_meta_command(InputBuffer* input_buffer, Table *table){
    if(strcmp((input_buffer->buffer), ".exit") == 0){
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }else if(strcmp((input_buffer->buffer), ".constants") == 0){
        printf("Constants: \n");
        print_constants();
        return META_COMMAND_SUCCESS;
    }else if(strcmp((input_buffer->buffer), ".btree") == 0){
        printf("Btree: \n");
        print_btree(table);
        return META_COMMAND_SUCCESS;
    }
    return META_COMMAND_UNRECOGNIZED;
}

void* get_cursor_value(Cursor* cursor){
    uint32_t page_num = cursor->page_num;

    void *node = get_page(cursor->table->pager, page_num);

    return leaf_node_value(node, cursor->cell_num);
}

void cursor_advance(Cursor* cursor){
    void *node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    cursor->cell_num += 1;
    if((cursor->cell_num) >= num_cells){
        cursor->end_of_table = true;
    }
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    void *node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS)
    {
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement->row_data);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor *cursor = table_find(table, key_to_insert);

    if(cursor->cell_num < num_cells){
        uint32_t present_key = *leaf_node_key(node, cursor->cell_num);
        if(present_key == key_to_insert){
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, key_to_insert, row_to_insert);

    free(cursor);

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table* table){
    Row row;
    Cursor *cursor = table_start(table);

    while(!(cursor->end_of_table)){
        void *row_slot = get_cursor_value(cursor);
        deserialize_row_data(&row, row_slot);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
        cursor_advance(cursor);
    }

    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table){
    switch (statement->type)
    {
    case STATEMENT_INSERT:
        printf("This will execute INSERT statement functionality... \n");
        return execute_insert(statement, table);
    case STATEMENT_SELECT:
        printf("This will execute SELECT statement functionality... \n");
        return execute_select(table);
    }
}

PrepareResult prepare_statment(InputBuffer* input_buffer,Statement* statement){
    if(strncmp(input_buffer->buffer, "insert", 6) == 0){
        statement->type = STATEMENT_INSERT;

        char* keyword = strtok(input_buffer->buffer, " ");
        char* id_string = strtok(NULL, " ");
        char* username = strtok(NULL, " ");
        char *email = strtok(NULL, " ");

        if(id_string == NULL || username == NULL || email == NULL){
            return PREPARE_SYNTAX_ERROR;
        }

        int id = atoi(id_string);
        if (id < 0)
        {
            return PREPARE_INVALID_ID;
        }

        if(strlen(username) > MAX_USERNAME_CHAR){
            return PREPARE_USERNAME_TOO_LONG;
        }
        if(strlen(email) > MAX_EMAIL_CHAR){
            return PREPARE_EMAIL_TOO_LONG;
        }

        statement->row_data.id = id;
        strcpy(statement->row_data.username, username);
        strcpy(statement->row_data.email, email);

        return PREPARE_SUCCESS;
    }else if(strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

int main(int argc, char* argv[]){
    if(argc < 2){
        printf("Error: db filename not provided!\n");
        exit(EXIT_FAILURE);
    }

    char *filename = argv[1];

    Table *table = open_db(filename);
    InputBuffer *input_buffer = create_new_buffer();

    while(true) {
        print_prompt();
        read_data_into_buffer(input_buffer);
        if(input_buffer->buffer[0] == '.'){
            switch(check_meta_command(input_buffer, table)){
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED):
                    printf("Unrecognized meta command %s \n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch(prepare_statment(input_buffer, &statement)){
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_INVALID_ID):
                printf("Error: Invalid userId! \n");
                continue;
            case (PREPARE_USERNAME_TOO_LONG):
                printf("Error: Username character length is too long! \n");
                continue;
            case (PREPARE_EMAIL_TOO_LONG):
                printf("Error: Email character length is too long! \n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax Error! Could not parse statement: %s \n", input_buffer->buffer);
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized Statement received %s \n", input_buffer->buffer);
                continue;
        }

        switch(execute_statement(&statement, table)){
            case (EXECUTE_SUCCESS):
                printf("Execution Succeeded!\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Table is completely full, no space left to add new row!\n");
                break;
            case (EXECUTE_FAILED):
                printf("Execution Failed!\n");
                break;
            case (EXECUTE_DUPLICATE_KEY):
                printf("Error: Duplicate Key already present in table: %d \n", statement.row_data.id);
                break;
            }
        printf("Command Executed! \n");
    }

    return 0;
}