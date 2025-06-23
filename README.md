# Noice Tip Processor

A typescript application that processes transactions for tips sent from/to [noice on farcaster](https://noice.so/). 

## What it does

This processor handles **batch transaction processing** - collecting pending transactions from the database and executing them efficiently in batches through an eip 7702 smart wallet. This approach significantly reduces gas costs compared to individual tip transaction processing.

**Key capabilities:**
- Processes ERC-20 token transfers using `transferFrom` operations
- Performs pre-execution validation with balance and allowance checks via multicall
- Includes retry mechanisms and comprehensive error logging
- Manages complete transaction lifecycle in PostgreSQL (pending → processing → completed/failed)

The system validates user balances and token allowances before processing to prevent failed transactions, and maintains detailed logs for troubleshooting.

## Getting Started

### Prerequisites

- Node.js (v18 or higher)
- PostgreSQL
- Ethereum wallet private key for server operations
- Base network RPC access

### Installation

```bash
git clone https://github.com/your-username/tip-processor.git
cd tip-processor
npm install
```

### Configuration

Create a `.env` file based on `.env.example`:
Database Schema
Set up the required PostgreSQL tables:

```sql

-- Users table
CREATE TABLE users (
    fid INT PRIMARY KEY,
    username VARCHAR(255),
    primary_eth_address VARCHAR(42),
);

-- Transactions table
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    from_fid INT NOT NULL,
    to_fid INT NOT NULL,
    amount VARCHAR(255) NOT NULL,           
    token_address VARCHAR(42) NOT NULL,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',  -- Lifecycle: pending → processing_in_batch → completed/failed
    tx_hash VARCHAR(66),
    error_message TEXT,
    transaction_receipt JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    delegated_address VARCHAR(42)
);
```
