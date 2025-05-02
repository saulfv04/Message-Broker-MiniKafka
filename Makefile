CC=gcc
CFLAGS=-Wall -Wextra -g -lpthread 
LDFLAGS=-pthread 

SRC_DIR=src
BIN_DIR=bin

BROKER_SRC=$(SRC_DIR)/broker.c
PRODUCER_SRC=$(SRC_DIR)/producer.c
CONSUMER_SRC=$(SRC_DIR)/consumer.c

BROKER_BIN=$(BIN_DIR)/broker
PRODUCER_BIN=$(BIN_DIR)/producer
CONSUMER_BIN=$(BIN_DIR)/consumer

all: $(BIN_DIR) $(BROKER_BIN) $(PRODUCER_BIN) $(CONSUMER_BIN)

$(BIN_DIR):
	@mkdir -p $(BIN_DIR)

$(BROKER_BIN): $(BROKER_SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(PRODUCER_BIN): $(PRODUCER_SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

$(CONSUMER_BIN): $(CONSUMER_SRC)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

run-broker: $(BROKER_BIN)
	$(BROKER_BIN)

clean:
	rm -rf $(BIN_DIR)
