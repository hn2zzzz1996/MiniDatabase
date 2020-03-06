db: main.c
	$(CC) main.c -g -o db -Wpointer-arith -pedantic -std=c99
