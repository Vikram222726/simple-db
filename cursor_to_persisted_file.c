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
    EXECUTE_TABLE_FULL
} ExecuteResult;

typedef struct {
    void *pages[MAX_TABLE_PAGES];
    uint32_t file_length;
    int file_descriptor;

} Pager;

typedef struct
{
    uint32_t rows_count;
    Pager *pager;
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
    uint32_t row_num;
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

    for (uint32_t i = 0; i < MAX_TABLE_PAGES; i++){
        pager->pages[i] = NULL;
    }

    return pager;
}

Cursor* table_start(Table* table){
    Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
    new_cursor->table = table;
    new_cursor->row_num = 0;
    new_cursor->end_of_table = (table->rows_count == 0);

    return new_cursor;
}

Cursor* table_end(Table* table){
    Cursor *new_cursor = (Cursor *)malloc(sizeof(Cursor));
    new_cursor->table = table;
    new_cursor->row_num = table->rows_count;
    new_cursor->end_of_table = true;

    return new_cursor;
}

Table* open_db(const char* filename){
    Pager *pager = initialize_pager(filename);

    Table *new_table = (Table *)malloc(sizeof(Table));
    new_table->pager = pager;
    new_table->rows_count = (pager->file_length) / ROW_SIZE;

    return new_table;
}

void flush_page_to_disk(Pager *pager, int page_num, uint32_t size){
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

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
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
    uint32_t num_pages_table = (table->rows_count) / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_pages_table;i++){
        if(pager->pages[i] == NULL){
            continue;
        }
        flush_page_to_disk(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    //Handle flushing of rows data for the last partial filled page as well..
    int partial_filled_page_rows = (table->rows_count) % ROWS_PER_PAGE;
    if(partial_filled_page_rows > 0){
        uint32_t last_page_num = num_pages_table;
        if(pager->pages[last_page_num] != NULL){
            flush_page_to_disk(pager, last_page_num, partial_filled_page_rows * ROW_SIZE);
            free(pager->pages[last_page_num]);
            pager->pages[last_page_num] = NULL;
        }
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

MetaCommandResult check_meta_command(InputBuffer* input_buffer, Table *table){
    if(strcmp((input_buffer->buffer), ".exit") == 0){
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    }
    return META_COMMAND_UNRECOGNIZED;
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
    }

    return pager->pages[page_num];
}

void* get_cursor_value(Cursor* cursor){
    uint32_t row_num = cursor->row_num;

    uint32_t page_num = row_num / ROWS_PER_PAGE;

    void *page = get_page(cursor->table->pager, page_num);

    uint32_t row_offset_in_page = row_num % ROWS_PER_PAGE;

    uint32_t bytes_offset = row_offset_in_page * ROW_SIZE;

    return page + bytes_offset;
}

void cursor_advance(Cursor* cursor){
    cursor->row_num++;

    if(cursor->row_num == cursor->table->rows_count){
        cursor->end_of_table = true;
    }
}

void serialize_row_data(Statement* statement, void* row_slot){
    memcpy(row_slot + ID_OFFSET, &(statement->row_data.id), ID_SIZE);
    memcpy(row_slot + USERNAME_OFFSET, &(statement->row_data.username), USERNAME_SIZE);
    memcpy(row_slot + EMAIL_OFFSET, &(statement->row_data.email), EMAIL_SIZE);
}

void deserialize_row_data(Row* destination,void* source){
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

ExecuteResult execute_insert(Statement* statement, Table* table){
    // Check if table is full..
    if(table->rows_count > TABLE_MAX_ROWS){
        return EXECUTE_TABLE_FULL;
    }

    Cursor *cursor = table_end(table);

    // If Table is not full, then for adding new row, get row_slot
    void *row_slot = get_cursor_value(cursor);

    // serialize and store data in this rowSlot location.
    serialize_row_data(statement, row_slot);
    table->rows_count += 1;

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
        }
        printf("Command Executed! \n");
    }

    return 0;
}