create database ethereum;
use ethereum;

CREATE TABLE blocks (
                        number BIGINT,
                        hash CHAR(66),
                        parent_hash CHAR(66),
                        nonce CHAR(18),
                        sha3_uncles CHAR(66),
                        logs_bloom VARCHAR(514),
                        transactions_root CHAR(66),
                        state_root CHAR(66),
                        receipts_root CHAR(66),
                        miner CHAR(42),
                        difficulty DECIMAL,
                        total_difficulty DECIMAL,
                        size BIGINT,
                        extra_data VARCHAR(2050),
                        gas_limit BIGINT,
                        gas_used BIGINT,
                        timestamp BIGINT,
                        transaction_count BIGINT,
                        base_fee_per_gas BIGINT
);

CREATE TABLE transactions (
                              hash CHAR(66),
                              nonce BIGINT,
                              block_hash CHAR(66),
                              block_number BIGINT,
                              transaction_index BIGINT,
                              from_address CHAR(42),
                              to_address CHAR(42),
                              value DECIMAL,
                              gas BIGINT,
                              gas_price BIGINT,
                              input VARCHAR(2050),
                              block_timestamp BIGINT,
                              max_fee_per_gas BIGINT,
                              max_priority_fee_per_gas BIGINT,
                              transaction_type BIGINT
);


CREATE TABLE token_transfers (
                                 token_address CHAR(42),
                                 from_address CHAR(42),
                                 to_address CHAR(42),
                                 value DECIMAL,
                                 transaction_hash CHAR(66),
                                 log_index BIGINT,
                                 block_number BIGINT
);

CREATE TABLE receipts (
                          transaction_hash CHAR(66),
                          transaction_index BIGINT,
                          block_hash CHAR(66),
                          block_number BIGINT,
                          cumulative_gas_used BIGINT,
                          gas_used BIGINT,
                          contract_address CHAR(42),
                          root CHAR(66),
                          status BIGINT,
                          effective_gas_price BIGINT
);

CREATE TABLE logs (
                      log_index BIGINT,
                      transaction_hash CHAR(66),
                      transaction_index BIGINT,
                      block_hash CHAR(66),
                      block_number BIGINT,
                      address CHAR(42),
                      data VARCHAR(2050),
                      topics TEXT
);

CREATE TABLE contracts (
                           address CHAR(42),
                           bytecode VARCHAR(2050),
                           function_sighashes TEXT,
                           is_erc20 BOOLEAN,
                           is_erc721 BOOLEAN,
                           block_number BIGINT
);

CREATE TABLE tokens (
                        address CHAR(42),
                        symbol TEXT,
                        name TEXT,
                        decimals BIGINT,
                        total_supply DECIMAL,
                        block_number BIGINT
);

CREATE TABLE traces (
                        block_number BIGINT,
                        transaction_hash CHAR(66),
                        transaction_index BIGINT,
                        from_address CHAR(42),
                        to_address CHAR(42),
                        value DECIMAL,
                        input VARCHAR(2050),
                        output VARCHAR(2050),
                        trace_type TEXT,
                        call_type TEXT,
                        reward_type TEXT,
                        gas BIGINT,
                        gas_used BIGINT,
                        subtraces BIGINT,
                        trace_address TEXT,
                        error TEXT,
                        status BIGINT,
                        trace_id TEXT
);