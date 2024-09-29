#include<stdbool.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<stdint.h>

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
    uint32_t rows_count;
    void *pages[MAX_TABLE_PAGES];
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

Table* initialize_table(){
    Table* new_table = (Table *)malloc(sizeof(Table));
    new_table->rows_count = 0;
    for (uint32_t i = 0; i < MAX_TABLE_PAGES; i++){
        new_table->pages[i] = NULL;
    }
    return new_table;
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

void free_table(Table *table){
    for (uint32_t i = 0; table->pages[i];i++){
        free(table->pages[i]);
    }
    free(table);
}

MetaCommandResult check_meta_command(InputBuffer* input_buffer, Table *table){
    if(strcmp((input_buffer->buffer), ".exit") == 0){
        close_input_buffer(input_buffer);
        free_table(table);
        exit(EXIT_SUCCESS);
    }
    return META_COMMAND_UNRECOGNIZED;
}

void* get_next_row_slot(Table *table, uint32_t row_num){
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    if(table->pages[page_num] == NULL){
        table->pages[page_num] = malloc(PAGE_SIZE);
    }
    void *page = table->pages[page_num];
    uint32_t row_offset_in_page = row_num % ROWS_PER_PAGE;
    uint32_t bytes_offset = row_offset_in_page * ROW_SIZE;

    return page + bytes_offset;
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

    // If Table is not full, then for adding new row, get row_slot
    void *row_slot = get_next_row_slot(table, table->rows_count);

    // serialize and store data in this rowSlot location.
    serialize_row_data(statement, row_slot);
    table->rows_count += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table* table){
    Row row;
    for (uint32_t i = 0; i < table->rows_count; i++)
    {
        void *row_slot = get_next_row_slot(table, i);
        deserialize_row_data(&row, row_slot);
        printf("(%d, %s, %s)\n", row.id, row.username, row.email);
    }

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
        // uint32_t result = sscanf(input_buffer->buffer, "insert %d %s %s", &(statement->row_data.id), statement->row_data.username, statement->row_data.email);
        // if (result < 3)
        // {
        //     return PREPARE_SYNTAX_ERROR;
        // }

        return PREPARE_SUCCESS;
    }else if(strncmp(input_buffer->buffer, "select", 6) == 0){
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

int main(){
    Table *table = initialize_table();
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