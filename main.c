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



MetaCommandResult do_meta_command(InputBuffer *input_buffer){
	if(!strcmp(input_buffer->buf, ".exit")){
		close_input_buffer(&input_buffer);
		printf("Bye.\n");
		exit(EXIT_SUCCESS);
	} else{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepare_command(InputBuffer *input_buffer, Statement *statement){
	if(!strncmp(input_buffer->buf, "insert", 6)){
		statement->type = STATEMENT_INSERT;
		return PREPARE_SUCCESS;
	} 
	if(!strncmp(input_buffer->buf, "select", 6)){
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement *statement){
	switch(statement->type){
		case STATEMENT_INSERT:
			printf("Do an insert.\n");
			break;
		case STATEMENT_SELECT:
			printf("Do an select.\n");
			break;
	}
}

int main(int argc, char *argv[]) {
	InputBuffer *input_buffer = new_input_buffer();

	while(1) {
		print_prompt();
		read_input(input_buffer);

		if(input_buffer->buf[0] == '.'){
			switch(do_meta_command(input_buffer)){
				case META_COMMAND_SUCCESS:
					continue;
				case META_COMMAND_UNRECOGNIZED_COMMAND:
					printf("Unrecognized command '%s'\n", input_buffer->buf);
					continue;
			}
		}

		Statement statement;
		switch(prepare_command(input_buffer, &statement)){
			case PREPARE_SUCCESS:
				break;
			case PREPARE_UNRECOGNIZED_STATEMENT:
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buf);
				continue;
		}

		execute_statement(&statement);
		printf("Executed Success.\n");
	}
}
