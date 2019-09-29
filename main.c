#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>

typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
	STATEMENT_INSERT,
	STATEMENT_SELECT
} StatementType;

typedef enum {
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL
} ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct Row{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE];
	char email[COLUMN_EMAIL_SIZE];
} Row;

typedef struct Statement {
	StatementType type;
	Row row_to_insert;	//only used by insert statement
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
#define ROW_SIZE  (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

#define ID_OFFSET (0)
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)

#define PAGE_SIZE (4096)
#define TABLE_MAX_PAGES 100
#define ROWS_PER_PAGE (PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)

typedef struct Table {
	uint32_t num_rows;
	void *pages[TABLE_MAX_PAGES];
} Table;

typedef struct InputBuffer {
	char *buf;
	size_t buf_len;
	ssize_t input_len;
} InputBuffer;

InputBuffer *new_input_buffer() {
	InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(*input_buffer));
	input_buffer->buf = NULL;
	input_buffer->buf_len = 0;
	input_buffer->input_len = 0;

	return input_buffer;
}

void close_input_buffer(InputBuffer **input_buffer) {
	if(*input_buffer) {
		free((*input_buffer)->buf);
		free(*input_buffer);
		*input_buffer = NULL;
	}
}


void print_prompt() {
	printf("db > ");
}

void read_input(InputBuffer *input_buffer) {
	ssize_t bytes_read = getline(&(input_buffer->buf), &(input_buffer->buf_len), stdin);
	
	if(bytes_read <= 0) {
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	input_buffer->buf_len = bytes_read - 1;
	input_buffer->buf[bytes_read - 1] = '\0';
}

Table *new_table() {
	Table *table = (Table *)malloc(sizeof(*table));
	table->num_rows = 0;
	int i;
	for(i = 0; i < TABLE_MAX_PAGES; i++){
		table->pages[i] = NULL;
	}
	return table;
}

void free_table(Table **table) {
	if(*table){
		int i;
		for(i = 0; i < TABLE_MAX_PAGES; i++){
			free((*table)->pages[i]);
		}
		free(*table);
		*table = NULL;
	}
}

void print_row(Row *row){
	printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void serialize_row(Row *row, void *dest){
	memcpy(dest+ID_OFFSET, &row->id, ID_SIZE);
	memcpy(dest+USERNAME_OFFSET, &row->username, USERNAME_SIZE);
	memcpy(dest+EMAIL_OFFSET, &row->email, EMAIL_SIZE);
}

void deserialize_row(void *src, Row *row){
	memcpy(&row->id, src+ID_OFFSET, ID_SIZE);
	memcpy(&row->username, src+USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&row->email, src+EMAIL_OFFSET, EMAIL_SIZE);
}

void *row_slot(Table *table, uint32_t row_num){
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void *page = table->pages[page_num];
	if(page == NULL){
		page = table->pages[page_num] = malloc(PAGE_SIZE);
	}
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

ExecuteResult execute_insert(Table *table, Statement *statement){
	if(table->num_rows >= TABLE_MAX_ROWS){
		return EXECUTE_TABLE_FULL;
	}

	Row *row_to_insert = &statement->row_to_insert;
	
	serialize_row(row_to_insert, row_slot(table, table->num_rows));
	table->num_rows += 1;

	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table *table, Statement *statement){
	Row row;
	for(uint32_t i = 0; i < table->num_rows; i++){
		deserialize_row(row_slot(table, i), &row);
		print_row(&row);
	}
	return EXECUTE_SUCCESS;
}

MetaCommandResult do_meta_command(Table *table, InputBuffer *input_buffer){
	if(!strcmp(input_buffer->buf, ".exit")){
		free_table(&table);
		close_input_buffer(&input_buffer);
		printf("Bye.\n");
		exit(EXIT_SUCCESS);
	} else{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement){
	if(!strncmp(input_buffer->buf, "insert", 6)){
		statement->type = STATEMENT_INSERT;
		int args_assigned = sscanf(input_buffer->buf, 
				"insert %d %s %s", &statement->row_to_insert.id, 
				statement->row_to_insert.username,
				statement->row_to_insert.email);
		if(args_assigned < 3){
			return PREPARE_SYNTAX_ERROR;
		}
		return PREPARE_SUCCESS;
	} 
	if(!strncmp(input_buffer->buf, "select", 6)){
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_statement(Table *table, Statement *statement){
	switch(statement->type){
		case STATEMENT_INSERT:
			return execute_insert(table, statement);
		case STATEMENT_SELECT:
			return execute_select(table, statement);
	}
}

int main(int argc, char *argv[]) {
	Table *table = new_table();
	InputBuffer *input_buffer = new_input_buffer();

	while(1) {
		print_prompt();
		read_input(input_buffer);

		if(input_buffer->buf[0] == '.'){
			switch(do_meta_command(table, input_buffer)){
				case META_COMMAND_SUCCESS:
					continue;
				case META_COMMAND_UNRECOGNIZED_COMMAND:
					printf("Unrecognized command '%s'\n", input_buffer->buf);
					continue;
			}
		}

		Statement statement;
		switch(prepare_statement(input_buffer, &statement)){
			case PREPARE_SUCCESS:
				break;
			case PREPARE_SYNTAX_ERROR:
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case PREPARE_UNRECOGNIZED_STATEMENT:
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buf);
				continue;
		}

		switch(execute_statement(table, &statement)){
			case EXECUTE_SUCCESS:
				printf("Executed\n");
				break;
			case EXECUTE_TABLE_FULL:
				printf("Error: Table full\n");
				break;
		}
	}

}
