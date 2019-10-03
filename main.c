#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <ctype.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>


typedef enum {
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
	PREPARE_SUCCESS,
	PREPARE_NEGATIVE_ID,
	PREPARE_STRING_TOO_LONG,
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
	char username[COLUMN_USERNAME_SIZE+1];
	char email[COLUMN_EMAIL_SIZE+1];
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

typedef struct {
	int file_descriptor;
	uint32_t file_length;
	void *pages[TABLE_MAX_PAGES];
} Pager;

typedef struct Table {
	Pager *pager;
	uint32_t num_rows;
} Table;

typedef struct {
	Table *table;
	uint32_t row_num;
	bool end_of_table;	
} Cursor;

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

Cursor *table_start(Table *table){
	Cursor *cursor = (Cursor *)malloc(sizeof(*cursor));

	cursor->table = table;
	cursor->row_num = 0;
	cursor->end_of_table = (table->num_rows == 0); 

	return cursor;
}

Cursor *table_end(Table *table){
	Cursor *cursor = (Cursor *)malloc(sizeof(*cursor));

	cursor->table = table;
	cursor->row_num = table->num_rows;
	cursor->end_of_table = true;

	return cursor;
}

void *get_page(Pager *pager, uint32_t page_num){
	if(page_num > TABLE_MAX_PAGES){
		printf("Tried to fetch page number out of bounds. %d > %d\n", page_num,
				TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}

	if(pager->pages[page_num] == NULL){
		void *page = malloc(PAGE_SIZE);
		uint32_t num_pages = pager->file_length / PAGE_SIZE;

		if(pager->file_length % PAGE_SIZE){
			num_pages += 1;
		}

		if(page_num <= num_pages){
			lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
			if(bytes_read == -1){
				printf("Error reading file: %d\n", errno);
				exit(EXIT_FAILURE);
			}
		}

		pager->pages[page_num] = page;
	}

	return pager->pages[page_num];
}

Pager *pager_open(const char *filename){
	int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);

	if(fd == -1){
		printf("Unable to open file\n");
		exit(EXIT_FAILURE);
	}

	off_t file_length = lseek(fd, 0, SEEK_END);
	
	Pager *pager = (Pager *)malloc(sizeof(*pager));
	pager->file_descriptor = fd;
	pager->file_length = file_length;

	for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
		pager->pages[i] = NULL;
	}

	return pager;
}

void pager_flush(Pager *pager, uint32_t page_num, uint32_t size){
	if(pager->pages[page_num] == NULL){
		printf("Tried to flush null page\n");
		exit(EXIT_FAILURE);
	}

	off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);

	if(offset == -1){
		printf("Error seeking: %d\n", errno);
		exit(EXIT_FAILURE);
	}

	ssize_t bytes_written = 
		write(pager->file_descriptor, pager->pages[page_num], size);

	if(bytes_written == -1){
		printf("Error writing: %d\n", errno);
		exit(EXIT_FAILURE);
	}
}

Table *db_open(const char *filename) {
	Pager *pager = pager_open(filename);
	uint32_t num_rows = pager->file_length / ROW_SIZE;

	Table *table = (Table *)malloc(sizeof(*table));
	table->pager = pager;
	table->num_rows = num_rows;
	return table;
}

void db_close(Table *table){
	Pager *pager = table->pager;
	uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

	for(uint32_t i = 0; i < num_full_pages; i++){
		if(pager->pages[i] == NULL){
			continue;
		}
		pager_flush(pager, i, PAGE_SIZE);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}

	uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
	if(num_additional_rows > 0){
		uint32_t page_num = num_full_pages;
		if(pager->pages[page_num] != NULL){
			pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
			free(pager->pages[page_num]);
			pager->pages[page_num] = NULL;
		}
	}

	int result = close(pager->file_descriptor);
	if(result == -1){
		printf("Error closing db file.\n");
		exit(EXIT_FAILURE);
	}

	for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
		void *page = pager->pages[i];
		if(page){
			free(page);
			pager->pages[i] = NULL;
		}
	}

	free(pager);
	free(table);
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

void *cursor_value(Cursor *cursor){
	uint32_t row_num = cursor->row_num;
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void *page = get_page(cursor->table->pager, page_num);
	uint32_t row_offset = row_num % ROWS_PER_PAGE;
	uint32_t byte_offset = row_offset * ROW_SIZE;
	return page + byte_offset;
}

void cursor_advance(Cursor *cursor){
	cursor->row_num += 1;
	if(cursor->row_num >= cursor->table->num_rows){
		cursor->end_of_table = true;
	}
}

ExecuteResult execute_insert(Table *table, Statement *statement){
	if(table->num_rows >= TABLE_MAX_ROWS){
		return EXECUTE_TABLE_FULL;
	}

	Row *row_to_insert = &statement->row_to_insert;
	Cursor *cursor = table_end(table);
	
	serialize_row(row_to_insert, cursor_value(cursor));
	table->num_rows += 1;

	free(cursor);

	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Table *table, Statement *statement){
	Cursor *cursor = table_start(table);
	Row row;
	
	while(!cursor->end_of_table){
		deserialize_row(cursor_value(cursor), &row);
		print_row(&row);
		cursor_advance(cursor);
	}

	free(cursor);
	return EXECUTE_SUCCESS;
}

MetaCommandResult do_meta_command(Table *table, InputBuffer *input_buffer){
	if(!strcmp(input_buffer->buf, ".exit")){
		db_close(table);
		close_input_buffer(&input_buffer);
		exit(EXIT_SUCCESS);
	} else{
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepare_insert(InputBuffer *input_buffer, Statement *statement){
	statement->type = STATEMENT_INSERT;
	
	char *keyword = strtok(input_buffer->buf, " ");
	char *id_string = strtok(NULL, " ");
	char *username = strtok(NULL, " ");
	char *email = strtok(NULL, " ");

	if(id_string == NULL || username == NULL || email == NULL){
		return PREPARE_SYNTAX_ERROR;
	}

	int id = atoi(id_string);
	if(id < 0){
		return PREPARE_NEGATIVE_ID;
	}
	if(strlen(username) > COLUMN_USERNAME_SIZE){
		return PREPARE_STRING_TOO_LONG; 
	}
	if(strlen(email) > COLUMN_EMAIL_SIZE){
		return PREPARE_STRING_TOO_LONG;
	}

	statement->row_to_insert.id = id;
	strcpy(statement->row_to_insert.username, username);
	strcpy(statement->row_to_insert.email, email);

	return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer, Statement *statement){
	if(!strncmp(input_buffer->buf, "insert", 6)){
		return prepare_insert(input_buffer, statement);
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
	if(argc < 2){
		printf("Must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}

	char *filename = argv[1];
	Table *table = db_open(filename);
	InputBuffer *input_buffer = new_input_buffer();

	while(1) {
		print_prompt();
		read_input(input_buffer);

		if(input_buffer->buf[0] == '.'){
			switch(do_meta_command(table, input_buffer)){
				case META_COMMAND_SUCCESS:
					continue;
				case META_COMMAND_UNRECOGNIZED_COMMAND:
					printf("Unrecognized command '%s'.\n", input_buffer->buf);
					continue;
			}
		}

		Statement statement;
		switch(prepare_statement(input_buffer, &statement)){
			case PREPARE_SUCCESS:
				break;
			case PREPARE_NEGATIVE_ID:
				printf("ID must be positive.\n");
				continue;
			case PREPARE_STRING_TOO_LONG:
				printf("String is too long.\n");
				continue;
			case PREPARE_SYNTAX_ERROR:
				printf("Syntax error. Could not parse statement.\n");
				continue;
			case PREPARE_UNRECOGNIZED_STATEMENT:
				printf("Unrecognized keyword at start of '%s'.\n", input_buffer->buf);
				continue;
		}

		switch(execute_statement(table, &statement)){
			case EXECUTE_SUCCESS:
				printf("Executed.\n");
				break;
			case EXECUTE_TABLE_FULL:
				printf("Error: Table full.\n");
				break;
		}
	}

}
