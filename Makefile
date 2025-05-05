CC=gcc
CFLAGS=-g -lpthread 
 

SRC_DIR=src
BIN_DIR=$(SRC_DIR)

BROKER_SRC=$(SRC_DIR)/broker.c
PRODUCER_SRC=$(SRC_DIR)/producer.c
CONSUMER_SRC=$(SRC_DIR)/consumer.c

BROKER_BIN=$(BIN_DIR)/broker
PRODUCER_BIN=$(BIN_DIR)/producer
CONSUMER_BIN=$(BIN_DIR)/consumer

all:  $(BROKER_BIN) $(PRODUCER_BIN) $(CONSUMER_BIN)

$(BROKER_BIN): $(BROKER_SRC)
	$(CC) $(CFLAGS) -o $@ $<

$(PRODUCER_BIN): $(PRODUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $< 

$(CONSUMER_BIN): $(CONSUMER_SRC)
	$(CC) $(CFLAGS) -o $@ $< 



clean:
	rm -f $(SRC_DIR)/broker $(SRC_DIR)/producer $(SRC_DIR)/consumer