CC=gcc
CFLAGS=-I/usr/include/CUnit
LDFLAGS=-lcunit

all: test_broker

test_broker: src/test_broker.c
	$(CC) $(CFLAGS) -o test_broker src/test_broker.c $(LDFLAGS)

run: test_broker
	./test_broker

clean:
	rm -f test_broker