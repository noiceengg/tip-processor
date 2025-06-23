

import {
    createPublicClient,
    createWalletClient,
    http,
    Hex,
    Address,
    parseAbi,
    encodeFunctionData,
    decodeFunctionResult,
    TransactionReceipt,
    getAddress,
    parseUnits,
    WaitForTransactionReceiptTimeoutError,
    TransactionNotFoundError,
    BaseError as ViemBaseError,
    ContractFunctionExecutionError,
  } from "viem";
  import { privateKeyToAccount } from "viem/accounts";
  import { base } from "viem/chains"; 
  import { Pool, QueryResult, PoolClient } from "pg";
  import { inspect } from "util"; 
  
  
  const DATABASE_URL = process.env.DATABASE_URL!;
  const BASE_RPC_URL = process.env.BASE_RPC_URL || "https:
  const SERVER_WALLET_PRIVATE_KEY = process.env.SERVER_WALLET_PRIVATE_KEY! as Hex;
  
  const SMART_WALLET_FOR_BATCHING_ADDRESS =
    process.env.SMART_WALLET_FOR_BATCHING_ADDRESS 
  const BATCH_SIZE_ONCHAIN_LIMIT = parseInt(
    process.env.TRANSACTION_BATCH_SIZE || "300",
    10,
  );
  const RECEIPT_TIMEOUT_SECONDS = parseInt(
    process.env.RECEIPT_TIMEOUT_SECONDS || "20",
    10,
  );
  const RATE_LOG_INTERVAL_SECONDS = parseInt(
    process.env.RATE_LOG_INTERVAL_SECONDS || "60",
    10,
  );
  const FETCH_CANDIDATE_LIMIT = parseInt(
    process.env.FETCH_CANDIDATE_LIMIT || "10",
    10,
  );
  const MAX_RETRIES_COUNT = parseInt(
    process.env.MAX_RETRIES_COUNT || "0", 
    10,
  );
  const ZERO_ADDRESS = "0x0000000000000000000000000000000000000000" as Address;
  
  const BASE_RPC_URL_WRITE = process.env.BASE_RPC_URL_WRITE || BASE_RPC_URL;
  
  const FINAL_BASE_RPC_URL_WRITE = BASE_RPC_URL_WRITE;
  
  
  const ERC20_ABI = parseAbi([
    "function transferFrom(address from, address to, uint256 value) external returns (bool)",
    "function decimals() external view returns (uint8)",
    "function allowance(address owner, address spender) external view returns (uint256)",
    "function balanceOf(address account) external view returns (uint256)",
    "function approve(address spender, uint256 value) external returns (bool)",
  ] as const);
  
  
  const SMART_WALLET_ABI = [
    {
      type: "function",
      name: "executeBatch",
      inputs: [
        {
          name: "calls",
          type: "tuple[]",
          components: [
            { name: "target", type: "address" },
            { name: "value", type: "uint256" },
            { name: "data", type: "bytes" },
          ],
        },
      ],
      outputs: [],
      stateMutability: "payable",
    },
    {
      type: "function",
      name: "isOwnerAddress",
      inputs: [{ name: "account", type: "address" }],
      outputs: [{ name: "", type: "bool" }],
      stateMutability: "view",
    },
  ] as const;
  
  
  const USDC_ADDRESS_BASE =
    "0x833589fcd6edb6e08f4c7c32d4f71b54bda02913" as Address;
  
  
  const MULTICALL_ABI = parseAbi([
    "function aggregate3((address target, bool allowFailure, bytes callData)[] calls) public payable returns ((bool success, bytes returnData)[] returnData)",
  ] as const);
  const MULTICALL_ADDRESS_BASE_MAINNET = (process.env.MULTICALL_ADDRESS ||
    "0xcA11bde05977b3631167028862bE2a173976CA11") as Address;
  
  
  export const publicClient = createPublicClient({
    chain: base,
    transport: http(BASE_RPC_URL),
  });
  
  export const walletClient = createWalletClient({
    chain: base,
    transport: http(FINAL_BASE_RPC_URL_WRITE),
  });
  
  let SERVER_WALLET_ADDRESS: Address;
  let serverAccount: ReturnType<typeof privateKeyToAccount>;
  
  
  try {
    if (!SERVER_WALLET_PRIVATE_KEY)
      throw new Error("SERVER_WALLET_PRIVATE_KEY environment variable not set.");
    serverAccount = privateKeyToAccount(SERVER_WALLET_PRIVATE_KEY);
    SERVER_WALLET_ADDRESS = serverAccount.address;
  } catch (e: any) {
    console.error(`Error initializing server wallet: ${e.message}`);
    process.exit(1); 
  }
  
  
  const SMART_WALLET_FOR_BATCHING_ADDRESS_CHECKSUMMED = getAddress(
    SMART_WALLET_FOR_BATCHING_ADDRESS,
  );
  const MULTICALL_ADDRESS_CHECKSUMMED = getAddress(
    MULTICALL_ADDRESS_BASE_MAINNET,
  );
  
  
  if (!DATABASE_URL) {
    console.error("Error: DATABASE_URL environment variable not set.");
    process.exit(1); 
  }
  
  
  console.log(`Server wallet address (EOA): ${SERVER_WALLET_ADDRESS}`);
  console.log(
    `Smart Wallet for Batching: ${SMART_WALLET_FOR_BATCHING_ADDRESS_CHECKSUMMED}`,
  );
  console.log(`Transaction Candidate Fetch Limit: ${FETCH_CANDIDATE_LIMIT}`);
  console.log(`On-chain Batch Size Limit: ${BATCH_SIZE_ONCHAIN_LIMIT}`);
  console.log(`Multicall Address: ${MULTICALL_ADDRESS_CHECKSUMMED}`);
  console.log(`Max Retries for Failed Transactions: ${MAX_RETRIES_COUNT}`);
  
  
  export const pool = new Pool({ connectionString: DATABASE_URL });
  
  
  pool.on("error", (err, client) => {
    console.error("Unexpected error on idle client", err);
    process.exit(-1); 
  });
  
  /**
   * Acquires a database connection from the pool.
   * @returns {Promise<PoolClient>} A Promise that resolves to a PostgreSQL client.
   * @throws {Error} If there's an error connecting to the database.
   */
  export async function getDbConnection(): Promise<PoolClient> {
    try {
      const client = await pool.connect();
      return client;
    } catch (e: any) {
      console.error(`Error connecting to database: ${e.message}`);
      throw e; 
    }
  }
  
  /**
   * Interface for User data retrieved from the database.
   */
  export interface User {
    fid: number;
    username: string | null;
    primary_eth_address: string | null;
    delegated_address: string | null;
  }
  
  
  const _userByFidCache = new Map<number, User | null>();
  
  /**
   * Retrieves user information by FID (Farcaster ID) from the database or cache.
   * Special handling for FID 0, mapping it to 1058691.
   * @param {PoolClient} dbClient - The PostgreSQL client.
   * @param {number} fid - The Farcaster ID of the user.
   * @returns {Promise<User | null>} A Promise that resolves to the User object or null if not found.
   */
  export async function getUserByFid(
    dbClient: PoolClient,
    fid: number,
  ): Promise<User | null> {
    
    if (_userByFidCache.has(fid)) {
      return _userByFidCache.get(fid)!;
    }
    
    let queryFid = fid;

    try {
      const result: QueryResult<User> = await dbClient.query(
        "SELECT fid, username, primary_eth_address, delegated_address FROM users WHERE fid = $1",
        [queryFid],
      );
      const user = result.rows[0] || null;
      _userByFidCache.set(fid, user); 
      if (fid === 0 && queryFid !== 0) {
        _userByFidCache.set(queryFid, user); 
      }
      return user;
    } catch (e: any) {
      console.error(`DB Error in getUserByFid for FID ${fid}: ${e.message}`);
      return null;
    }
  }
  
  
  const _tokenDecimalsCache = new Map<Address, number>();
  
  /**
   * Fetches the number of decimals for an ERC20 token.
   * Uses a cache to avoid repeated on-chain calls.
   * Defaults to 6 for USDC and 18 for others if decimals cannot be fetched.
   * @param {Address} tokenAddressHex - The hexadecimal address of the token.
   * @returns {Promise<number>} A Promise that resolves to the number of decimals.
   */
  export async function getTokenDecimals(tokenAddressHex: Address): Promise<number> {
    const tokenAddressChecksum = getAddress(tokenAddressHex);
    if (_tokenDecimalsCache.has(tokenAddressChecksum)) {
      return _tokenDecimalsCache.get(tokenAddressChecksum)!;
    }
  
    try {
      const decimals = await publicClient.readContract({
        address: tokenAddressChecksum,
        abi: ERC20_ABI,
        functionName: "decimals",
      });
      _tokenDecimalsCache.set(tokenAddressChecksum, decimals);
      return decimals;
    } catch (e: any) {
      console.error(
        `Error fetching decimals for ${tokenAddressChecksum}: ${e.message}. Attempting default.`,
      );
      
      if (
        tokenAddressChecksum.toLowerCase() === USDC_ADDRESS_BASE.toLowerCase()
      ) {
        _tokenDecimalsCache.set(tokenAddressChecksum, 6);
        return 6;
      }
      console.warn(
        `Warning: Could not determine decimals for ${tokenAddressChecksum}. Defaulting to 18.`,
      );
      _tokenDecimalsCache.set(tokenAddressChecksum, 18); 
      return 18;
    }
  }
  
  /**
   * Serializes a Viem TransactionReceipt object into a JSON string suitable for database storage.
   * Handles BigInt values by converting them to strings.
   * @param {TransactionReceipt | null | undefined} receipt - The transaction receipt.
   * @returns {string | null} The JSON string representation of the receipt, or null if input is null/undefined.
   */
  function serializeReceiptForDb(
    receipt: TransactionReceipt | null | undefined,
  ): string | null {
    if (!receipt) return null;
    
    const replacer = (key: string, value: any) =>
      typeof value === "bigint" ? value.toString() : value;
    return JSON.stringify(receipt, replacer);
  }
  
  /**
   * Interface for a transaction record in the database.
   */
  export interface TransactionRecord {
    id: number;
    from_fid: number;
    to_fid: number;
    amount: string; 
    token_address: Address;
    type: string;
    status: string; 
    tx_hash: Hex | null;
    error_message: string | null;
    transaction_receipt: any | null; 
    created_at: Date;
    updated_at: Date;
    retry_count: number;
    delegated_address?: Address | null; 
  }
  
  /**
   * Updates a single transaction record in the database.
   * Handles incrementing retry_count and setting 'permanently_failed' status if MAX_RETRIES_COUNT is exceeded.
   * @param {PoolClient} dbClient - The PostgreSQL client.
   * @param {number} txId - The ID of the transaction to update.
   * @param {TransactionRecord["status"]} newStatus - The new status of the transaction.
   * @param {Hex | null} [txHash=null] - The transaction hash (optional).
   * @param {string | null} [errorMessage=null] - An error message if the transaction failed (optional).
   * @param {TransactionReceipt | null} [receiptData=null] - The transaction receipt data (optional).
   * @returns {Promise<void>}
   * @throws {Error} If there's a database error.
   */
  export async function updateTransactionInDb(
    dbClient: PoolClient,
    txId: number,
    newStatus: TransactionRecord["status"],
    txHash: Hex | null = null,
    errorMessage: string | null = null,
    receiptData: TransactionReceipt | null = null,
  ): Promise<void> {
    try {
      const jsonReceiptData = serializeReceiptForDb(receiptData);
  
      if (newStatus === "failed") {
        
        const res = await dbClient.query(
          "SELECT retry_count FROM transactions WHERE id = $1 FOR UPDATE", 
          [txId],
        );
        const currentRetryCount = (res.rows[0]?.retry_count ?? 0) as number;
        const updatedRetryCount = currentRetryCount + 1;
        let finalStatusForFailure: TransactionRecord["status"] = "failed";
  
        if (updatedRetryCount > MAX_RETRIES_COUNT) {
          finalStatusForFailure = "permanently_failed";
          console.log(
            `TxID ${txId}: Max retries (${MAX_RETRIES_COUNT}) reached. Error: '${errorMessage}'. Setting status to '${finalStatusForFailure}'.`,
          );
        } else {
          console.log(
            `TxID ${txId}: Failed (attempt ${updatedRetryCount}/${MAX_RETRIES_COUNT + 1}). Error: '${errorMessage}'. Status set to '${finalStatusForFailure}' for potential retry.`,
          );
        }
        await dbClient.query(
          `UPDATE transactions
                   SET status = $1, error_message = $2, tx_hash = $3,
                       transaction_receipt = $4, retry_count = $5, updated_at = NOW()
                   WHERE id = $6`,
          [
            finalStatusForFailure,
            errorMessage,
            txHash,
            jsonReceiptData,
            updatedRetryCount,
            txId,
          ],
        );
      } else if (newStatus === "completed") {
        await dbClient.query(
          `UPDATE transactions
                   SET status = $1, tx_hash = $2, error_message = NULL,
                       transaction_receipt = $3, updated_at = NOW()
                   WHERE id = $4`,
          [newStatus, txHash, jsonReceiptData, txId],
        );
      } else {
        
        await dbClient.query(
          `UPDATE transactions
                   SET status = $1, tx_hash = $2, error_message = $3, updated_at = NOW()
                   WHERE id = $4`,
          [newStatus, txHash, errorMessage, txId],
        );
      }
    } catch (e: any) {
      console.error(
        `DB Error in updateTransactionInDb for TxID ${txId} (New Status: ${newStatus}): ${e.constructor.name} - ${e.message}`,
      );
      throw e; 
    }
  }
  
  /**
   * Fails all pending or previously failed transactions for a specific user (from_fid) and token.
   * This is typically used when a user's on-chain balance or allowance is insufficient.
   * @param {PoolClient} dbClient - The PostgreSQL client.
   * @param {number} fid - The Farcaster ID of the sender.
   * @param {Address} tokenAddressHex - The token address.
   * @param {string} errorMessage - The error message to record.
   * @returns {Promise<number>} The number of rows updated.
   * @throws {Error} If there's a database error.
   */
  export async function failAllPendingByFidToken(
    dbClient: PoolClient,
    fid: number,
    tokenAddressHex: Address,
    errorMessage: string,
  ): Promise<number> {
    if (!fid) {
      console.warn(
        `Warning: Attempted to bulk fail by FID/Token for invalid FID ${fid}.`,
      );
      if (fid === 0) {
        console.log(
          `DB (Bulk Fail by FID/Token): Skipped for FID 0, Token ${tokenAddressHex}.`,
        );
        return 0;
      }
    }
    if (!tokenAddressHex) {
      console.error(
        "Error: Cannot fail transactions for an invalid token_address.",
      );
      return 0;
    }
  
    const sqlUpdate = `
          UPDATE transactions
          SET
              status = CASE
                           WHEN retry_count + 1 > $1 THEN 'permanently_failed'::text
                           ELSE 'failed'::text
                       END,
              error_message = $2,
              retry_count = retry_count + 1,
              updated_at = NOW()
          WHERE
              from_fid = $3 AND
              lower(token_address) = lower($4) AND
              status IN ('pending', 'failed');
      `;
    try {
      const result = await dbClient.query(sqlUpdate, [
        MAX_RETRIES_COUNT, 
        errorMessage,      
        fid,               
        tokenAddressHex,   
      ]);
      const updatedCount = result.rowCount || 0;
      if (updatedCount > 0) {
        console.log(
          `DB (Bulk Fail by FID/Token): Successfully updated ${updatedCount} transactions for FID ${fid}, Token ${tokenAddressHex.slice(0, 10)}...`,
        );
      }
      return updatedCount;
    } catch (e: any) {
      console.error(
        `DB Error in failAllPendingByFidToken for FID ${fid}, Token ${tokenAddressHex}: ${e.constructor.name} - ${e.message}`,
      );
      throw e; 
    }
  }
  
  /**
   * Optimizes updating status for multiple transaction records in the database, typically after a batch transaction.
   * @param {PoolClient} dbClient - The PostgreSQL client.
   * @param {number[]} txIds - An array of transaction IDs to update.
   * @param {TransactionRecord["status"]} newStatus - The new status for the transactions.
   * @param {Hex | null} [batchTxHash=null] - The hash of the batch transaction (optional).
   * @param {string | null} [errorMessage=null] - An error message if the batch failed (optional).
   * @param {TransactionReceipt | null} [receiptData=null] - The batch transaction receipt data (optional).
   * @returns {Promise<void>}
   * @throws {Error} If there's a database error.
   */
  export async function updateBatchTransactionsInDbOptimized(
    dbClient: PoolClient,
    txIds: number[],
    newStatus: TransactionRecord["status"],
    batchTxHash: Hex | null = null,
    errorMessage: string | null = null,
    receiptData: TransactionReceipt | null = null,
  ): Promise<void> {
    if (!txIds || txIds.length === 0) {
      return;
    }
  
    console.log(
      `DB (Optimized): Updating ${txIds.length} transactions to status '${newStatus}' for batch hash ${batchTxHash || "N/A"}.`,
    );
    const jsonReceiptData = serializeReceiptForDb(receiptData);
  
    try {
      if (newStatus === "failed") {
        const sqlFailedUpdate = `
                  UPDATE transactions
                  SET
                      status = CASE
                                   WHEN retry_count + 1 > $1 THEN 'permanently_failed'::text
                                   ELSE 'failed'::text
                               END,
                      error_message = $2,
                      tx_hash = $3,
                      transaction_receipt = $4,
                      retry_count = retry_count + 1,
                      updated_at = NOW()
                  WHERE id = ANY($5::int[]); -- Use int[] for array of numbers
              `;
        const result = await dbClient.query(sqlFailedUpdate, [
          MAX_RETRIES_COUNT,
          errorMessage,
          batchTxHash,
          jsonReceiptData,
          txIds,
        ]);
        console.log(
          `TxIDs ${txIds.join(",")}: Bulk updated to 'failed' or 'permanently_failed'. Error: '${errorMessage}'. Affected rows: ${result.rowCount}`,
        );
      } else if (newStatus === "completed") {
        const sqlCompletedUpdate = `
                  UPDATE transactions
                  SET status = $1, tx_hash = $2, error_message = NULL,
                      transaction_receipt = $3, updated_at = NOW()
                  WHERE id = ANY($4::int[]);
              `;
        await dbClient.query(sqlCompletedUpdate, [
          newStatus,
          batchTxHash,
          jsonReceiptData,
          txIds,
        ]);
      } else if (newStatus === "processing_in_batch") {
        const sqlOtherStatusUpdate = `
                  UPDATE transactions
                  SET status = $1, tx_hash = $2, error_message = $3, updated_at = NOW()
                  WHERE id = ANY($4::int[]);
              `;
        await dbClient.query(sqlOtherStatusUpdate, [
          newStatus,
          batchTxHash,
          errorMessage,
          txIds,
        ]);
      } else {
        
        console.warn(
          `Warning: Unhandled bulk status update for '${newStatus}'. Falling back to individual updates. This should not happen.`,
        );
        for (const txId of txIds) {
          await updateTransactionInDb(
            dbClient,
            txId,
            newStatus,
            batchTxHash,
            errorMessage,
            receiptData,
          );
        }
      }
    } catch (e: any) {
      console.error(
        `DB Error in updateBatchTransactionsInDbOptimized for status '${newStatus}': ${e.constructor.name} - ${e.message}`,
      );
      throw e; 
    }
  }
  
  /**
   * Interface for the initial state (balance and allowance) of a user-token pair.
   */
  interface InitialState {
    balance: bigint;
    allowance: bigint;
    error: boolean; 
    balanceFetchedSuccessfully: boolean;
    allowanceFetchedSuccessfully: boolean;
  }
  
  /**
   * Fetches initial on-chain balances and allowances for multiple user-token pairs using a multicall contract.
   * This optimizes fetching many states in a single RPC call.
   * @param {Map<string, { userAddrCs: Address; tokenAddrCs: Address }>} userTokenPairsToFetch - Map of user-token pairs to fetch. Key: `${userAddrCs}-${tokenAddrCs}`.
   * @param {Address} smartWalletCsAddr - The checksummed address of the smart wallet (spender).
   * @returns {Promise<Map<string, InitialState>>} A Map containing the fetched states for each pair.
   */
  export async function fetchInitialStatesMulticall(
    userTokenPairsToFetch: Map<
      string,
      { userAddrCs: Address; tokenAddrCs: Address }
    >, 
    smartWalletCsAddr: Address,
  ): Promise<Map<string, InitialState>> {
    const fetchedStates = new Map<string, InitialState>();
  
    if (userTokenPairsToFetch.size === 0) return fetchedStates;
  
    const calls: { target: Address; allowFailure: boolean; callData: Hex }[] = [];
    const callKeysMap: {
      type: "balance" | "allowance";
      user: Address;
      token: Address;
    }[] = [];
  
    
    for (const { userAddrCs, tokenAddrCs } of userTokenPairsToFetch.values()) {
      const pairKey = `${userAddrCs}-${tokenAddrCs}`;
      
      fetchedStates.set(pairKey, {
        balance: 0n,
        allowance: 0n,
        error: true, 
        balanceFetchedSuccessfully: false,
        allowanceFetchedSuccessfully: false,
      });
  
      try {
        
        const balanceCallData = encodeFunctionData({
          abi: ERC20_ABI,
          functionName: "balanceOf",
          args: [userAddrCs],
        });
        
        const allowanceCallData = encodeFunctionData({
          abi: ERC20_ABI,
          functionName: "allowance",
          args: [userAddrCs, smartWalletCsAddr],
        });
  
        calls.push({
          target: tokenAddrCs,
          allowFailure: true, 
          callData: balanceCallData,
        });
        callKeysMap.push({
          type: "balance",
          user: userAddrCs,
          token: tokenAddrCs,
        });
  
        calls.push({
          target: tokenAddrCs,
          allowFailure: true,
          callData: allowanceCallData,
        });
        callKeysMap.push({
          type: "allowance",
          user: userAddrCs,
          token: tokenAddrCs,
        });
      } catch (e: any) {
        console.error(
          `CRITICAL: ABI encoding failed for ${userAddrCs}/${tokenAddrCs}: ${e.message}`,
        );
        
      }
    }
  
    if (calls.length === 0) {
      console.warn("Multicall: No valid calls generated for execution.");
      return fetchedStates; 
    }
  
    try {
      
      
      
      const results: any[] = await publicClient.readContract({
        address: MULTICALL_ADDRESS_CHECKSUMMED,
        abi: MULTICALL_ABI,
        functionName: "aggregate3",
        args: [calls],
      });
  
      
      results.forEach((result: { success: boolean; returnData: Hex }, i: number) => {
        const mappingInfo = callKeysMap[i];
        const pairKey = `${mappingInfo.user}-${mappingInfo.token}`;
        const state = fetchedStates.get(pairKey);
        if (!state) return; 
  
        if (!result.success) {
          state.error = true;
          console.warn(
            `Multicall sub-call failed for ${pairKey} (${mappingInfo.type}). Return Data: ${result.returnData}`,
          );
          return; 
        }
  
        try {
          if (mappingInfo.type === "balance") {
            const decodedBalance = decodeFunctionResult({
              abi: ERC20_ABI,
              functionName: "balanceOf",
              data: result.returnData,
            });
            state.balance = decodedBalance as bigint;
            state.balanceFetchedSuccessfully = true;
          } else if (mappingInfo.type === "allowance") {
            const decodedAllowance = decodeFunctionResult({
              abi: ERC20_ABI,
              functionName: "allowance",
              data: result.returnData,
            });
            state.allowance = decodedAllowance as bigint;
            state.allowanceFetchedSuccessfully = true;
          }
          
        } catch (e_decode: any) {
          state.error = true;
          console.error(
            `Multicall decode error for ${pairKey} (${mappingInfo.type}): ${e_decode.message}. Data: ${result.returnData}`,
          );
        }
      });
    } catch (e_multicall: any) {
      console.error(
        `Multicall aggregate3 RPC call error: ${e_multicall.constructor.name} - ${e_multicall.message}`,
      );
      
      userTokenPairsToFetch.forEach(({ userAddrCs, tokenAddrCs }) => {
        const pairKey = `${userAddrCs}-${tokenAddrCs}`;
        const state = fetchedStates.get(pairKey);
        if (state) state.error = true;
      });
    }
  
    
    fetchedStates.forEach((stateData, pairKey) => {
      if (
        stateData.error || 
        !stateData.balanceFetchedSuccessfully ||
        !stateData.allowanceFetchedSuccessfully
      ) {
        stateData.error = true;
      } else {
        stateData.error = false; 
      }
    });
    return fetchedStates;
  }
  
  /**
   * Processes a batch of candidate transactions.
   * Performs validation (balance, allowance), aggregates valid transactions into a single on-chain batch call,
   * sends the call to the smart wallet, and updates transaction statuses in the database.
   * @param {PoolClient} dbClient - The PostgreSQL client.
   * @param {TransactionRecord[]} txRecordsList - List of candidate transaction records from the database.
   * @returns {Promise<number>} The number of transactions actually attempted on-chain in this batch.
   */
  export async function processBatchTransactions(
    dbClient: PoolClient,
    txRecordsList: TransactionRecord[],
  ): Promise<number> {
    const batchProcessingStartTime = Date.now();
    _userByFidCache.clear(); 
  
    console.log(
      `\nProcessing Batch of ${txRecordsList.length} candidate TxIDs...`,
    );
  
    
    const callsForSmartWallet: { target: Address; value: bigint; data: Hex }[] =
      [];
    
    const txIdsInFinalCalls: number[] = [];
  
    
    const spentBalancesInBatch = new Map<string, bigint>(); 
    const spentAllowancesInBatch = new Map<string, bigint>(); 
    
    const skippedUserTokenPairsInBatch = new Map<string, boolean>(); 
  
    
    const tempTxDataForProcessing: (TransactionRecord & {
      txDbId: number;
      fromAddressHexCs: Address;
      tokenAddressHexCs: Address;
    })[] = [];
  
    
    const userTokenPairsToFetch = new Map<
      string,
      { userAddrCs: Address; tokenAddrCs: Address }
    >();
  
    
    for (const txRecord of txRecordsList) {
      const txDbId = txRecord.id;
      try {
        const tokenAddressHexCs = getAddress(txRecord.token_address);
        const fromFid = txRecord.from_fid;
        const fromUser = await getUserByFid(dbClient, fromFid);
  
        if (!fromUser || !fromUser.primary_eth_address) {
          const errorMsg = `Sender FID ${fromFid} (TxID ${txDbId}) missing primary_eth_address.`;
          await updateTransactionInDb(dbClient, txDbId, "failed", null, errorMsg);
          continue; 
        }
        const fromAddressHexCs = getAddress(fromUser.primary_eth_address);
        const pairKey = `${fromAddressHexCs}-${tokenAddressHexCs}`;
        userTokenPairsToFetch.set(pairKey, {
          userAddrCs: fromAddressHexCs,
          tokenAddrCs: tokenAddressHexCs,
        });
        tempTxDataForProcessing.push({
          ...txRecord,
          txDbId,
          fromAddressHexCs,
          tokenAddressHexCs,
        });
      } catch (e: any) {
        const errorMsg = e instanceof Error ? e.message : String(e);
        console.error(
          `Initial prep error for TxID ${txDbId}: ${errorMsg}. Marking failed.`,
        );
        await updateTransactionInDb(
          dbClient,
          txDbId,
          "failed",
          null,
          `Prep error: ${errorMsg}`,
        );
      }
    }
  
    if (tempTxDataForProcessing.length === 0) {
      console.log(
        "Batch: No transactions passed initial preparation for pre-fetch.",
      );
      return 0; 
    }
  
    
    const initialUserTokenStates = await fetchInitialStatesMulticall(
      userTokenPairsToFetch,
      SMART_WALLET_FOR_BATCHING_ADDRESS_CHECKSUMMED,
    );
  
    
    for (const txData of tempTxDataForProcessing) {
      const {
        txDbId,
        fromAddressHexCs,
        tokenAddressHexCs,
        from_fid,
        to_fid,
        amount: amountStr,
      } = txData;
      const userTokenKey = `${fromAddressHexCs}-${tokenAddressHexCs}`;
      const logPrefix = `  TxID ${txDbId} (${from_fid}->${to_fid}, ${amountStr} ${tokenAddressHexCs.slice(0, 6)}..)`;
  
      
      if (skippedUserTokenPairsInBatch.get(userTokenKey)) {
        
        continue;
      }
  
      const currentOnchainStates = initialUserTokenStates.get(userTokenKey);
      
      if (
        !currentOnchainStates ||
        currentOnchainStates.error || 
        !currentOnchainStates.balanceFetchedSuccessfully || 
        !currentOnchainStates.allowanceFetchedSuccessfully
      ) {
        const errorMsg = `On-chain state fetch failed or incomplete for ${userTokenKey}.`;
        console.warn(`${logPrefix}: ${errorMsg}`);
        await updateTransactionInDb(dbClient, txDbId, "failed", null, errorMsg);
        
        skippedUserTokenPairsInBatch.set(userTokenKey, true);
        await failAllPendingByFidToken(
          dbClient,
          from_fid,
          tokenAddressHexCs,
          errorMsg + " (cascaded from prior on-chain state fetch error)",
        );
        continue; 
      }
  
      try {
        const toUser = await getUserByFid(dbClient, to_fid);
        let toAddressCs: Address;
  

          if (!toUser || !toUser.primary_eth_address) {
            throw new Error(`Recipient FID ${to_fid} has no primary_eth_address.`);
          }
          toAddressCs = getAddress(toUser.primary_eth_address);
          if (toAddressCs === ZERO_ADDRESS) {
            throw new Error(
              `Recipient FID ${to_fid} resolved to the zero address (${toAddressCs}).`,
            );
          }
        
  
        const decimals = await getTokenDecimals(tokenAddressHexCs);
        const amountInSmallestUnit = parseUnits(amountStr, decimals); 
  
        if (amountInSmallestUnit <= 0n) {
          throw new Error(`Amount must be greater than 0. Parsed: ${amountInSmallestUnit}`);
        }
  
        const currentSpentBalance = spentBalancesInBatch.get(userTokenKey) || 0n;
        const effectiveRemainingBalance =
          currentOnchainStates.balance - currentSpentBalance;
  
        if (effectiveRemainingBalance < amountInSmallestUnit) {
          const errorMsg = `Insufficient balance: Needs ${amountInSmallestUnit}, effective ${effectiveRemainingBalance} (Initial: ${currentOnchainStates.balance}, BatchSpent: ${currentSpentBalance}).`;
          await updateTransactionInDb(dbClient, txDbId, "failed", null, errorMsg);
          if (!skippedUserTokenPairsInBatch.get(userTokenKey)) {
            await failAllPendingByFidToken(
              dbClient,
              from_fid,
              tokenAddressHexCs,
              errorMsg,
            );
            skippedUserTokenPairsInBatch.set(userTokenKey, true);
          }
          continue; 
        }
  
        const currentSpentAllowance =
          spentAllowancesInBatch.get(userTokenKey) || 0n;
        const effectiveRemainingAllowance =
          currentOnchainStates.allowance - currentSpentAllowance;
  
        if (effectiveRemainingAllowance < amountInSmallestUnit) {
          const errorMsg = `Insufficient allowance: Needs ${amountInSmallestUnit}, effective ${effectiveRemainingAllowance} (Initial: ${currentOnchainStates.allowance}, BatchSpent: ${currentSpentAllowance}).`;
          await updateTransactionInDb(dbClient, txDbId, "failed", null, errorMsg);
          if (!skippedUserTokenPairsInBatch.get(userTokenKey)) {
            await failAllPendingByFidToken(
              dbClient,
              from_fid,
              tokenAddressHexCs,
              errorMsg,
            );
            skippedUserTokenPairsInBatch.set(userTokenKey, true);
          }
          continue;
        }
  
        if (
          BATCH_SIZE_ONCHAIN_LIMIT > 0 &&
          callsForSmartWallet.length >= BATCH_SIZE_ONCHAIN_LIMIT
        ) {
          console.log(
            `${logPrefix}: SKIPPED (batch on-chain limit ${BATCH_SIZE_ONCHAIN_LIMIT} reached). Tx will be picked in next cycle.`,
          );
          continue; 
        }
  
        
        const callData = encodeFunctionData({
          abi: ERC20_ABI,
          functionName: "transferFrom",
          args: [fromAddressHexCs, toAddressCs, amountInSmallestUnit],
        });
  
        
        callsForSmartWallet.push({
          target: tokenAddressHexCs,
          value: 0n, 
          data: callData,
        });
  
        
        spentBalancesInBatch.set(
          userTokenKey,
          currentSpentBalance + amountInSmallestUnit,
        );
        spentAllowancesInBatch.set(
          userTokenKey,
          currentSpentAllowance + amountInSmallestUnit,
        );
        txIdsInFinalCalls.push(txDbId);
        
      } catch (e: any) {
        const errorMsg = e instanceof Error ? e.message : String(e);
        console.warn(`${logPrefix}: Validation/Loop error: ${errorMsg}`);
        await updateTransactionInDb(
          dbClient,
          txDbId,
          "failed",
          null,
          `Loop error: ${errorMsg}`,
        );
      }
    }
  
    if (callsForSmartWallet.length === 0) {
      console.log(
        "Batch: No calls eligible for on-chain execution after all validations.",
      );
      return 0;
    }
  
    let currentBatchTxHashHex: Hex | null = null;
    let batchReceipt: TransactionReceipt | null = null;
  
    
    try {
      
      await updateBatchTransactionsInDbOptimized(
        dbClient,
        txIdsInFinalCalls,
        "processing_in_batch",
        null,
        "Marked for batch processing",
      );
  
      let gasLimit: bigint;
      try {
        
        const { request } = await publicClient.simulateContract({
          account: serverAccount, 
          address: SMART_WALLET_FOR_BATCHING_ADDRESS_CHECKSUMMED,
          abi: SMART_WALLET_ABI,
          functionName: "executeBatch",
          args: [callsForSmartWallet],
          value: 0n, 
        });
        
        gasLimit = request.gas ? (request.gas * 130n) / 100n : 300000n; 
        
        gasLimit = gasLimit > 200000n + 150000n * BigInt(callsForSmartWallet.length) 
          ? gasLimit 
          : 200000n + 150000n * BigInt(callsForSmartWallet.length);
      } catch (e: any) {
        console.warn(
          `Gas estimation failed (${e.message}). Falling back to fixed gas limit.`,
        );
        
        gasLimit = 200000n + 150000n * BigInt(callsForSmartWallet.length);
      }
  
      
      const nonce = await publicClient.getTransactionCount({
        address: SERVER_WALLET_ADDRESS,
        blockTag: "pending", 
      });
  
      console.log(
        "Submitting batch with calls:",
        inspect(callsForSmartWallet, { depth: 3 }),
      );
      console.log(
        `Using EOA: ${SERVER_WALLET_ADDRESS}, Nonce: ${nonce}, Estimated Gas Limit: ${gasLimit}`,
      );
  
      
      currentBatchTxHashHex = await walletClient.writeContract({
        account: serverAccount,
        address: SMART_WALLET_FOR_BATCHING_ADDRESS_CHECKSUMMED,
        abi: SMART_WALLET_ABI,
        functionName: "executeBatch",
        args: [callsForSmartWallet],
        value: 0n,
        gas: gasLimit,
        nonce: nonce, 
      });
  
      console.log(
        `On-Chain: Batch sent: ${currentBatchTxHashHex}. Waiting for receipt...`,
      );
      
      await updateBatchTransactionsInDbOptimized(
        dbClient,
        txIdsInFinalCalls,
        "processing_in_batch",
        currentBatchTxHashHex,
        "Batch submitted to network",
      );
  
      
      batchReceipt = await publicClient.waitForTransactionReceipt({
        hash: currentBatchTxHashHash,
        timeout: RECEIPT_TIMEOUT_SECONDS * 1000, 
      });
  
      console.log(
        `Batch Tx Receipt for ${currentBatchTxHashHex}: Status: ${batchReceipt.status}, Block: ${batchReceipt.blockNumber}, GasUsed: ${batchReceipt.gasUsed}`,
      );
  
      
      if (batchReceipt.status === "success") {
        console.log(
          `On-Chain Success: Batch ${currentBatchTxHashHex} confirmed successfully.`,
        );
        await updateBatchTransactionsInDbOptimized(
          dbClient,
          txIdsInFinalCalls,
          "completed",
          currentBatchTxHashHex,
          null, 
          batchReceipt,
        );
      } else {
        
        const errorMsg = `On-Chain Revert: Batch ${currentBatchTxHashHex} (status ${batchReceipt.status}). Individual transactions will be marked failed for retry.`;
        console.error(errorMsg);
        await updateBatchTransactionsInDbOptimized(
          dbClient,
          txIdsInFinalCalls,
          "failed",
          currentBatchTxHashHex,
          errorMsg,
          batchReceipt,
        );
      }
    } catch (e: any) {
      
      let errorMsg = `Batch processing error: ${e.message}`;
      if (e instanceof WaitForTransactionReceiptTimeoutError) {
        errorMsg = `Timeout/NotFound for batch ${currentBatchTxHashHex || "N/A"}: Transaction receipt not received within ${RECEIPT_TIMEOUT_SECONDS}s. Marked for retry.`;
      } else if (e instanceof TransactionNotFoundError) {
        errorMsg = `TransactionNotFound for batch ${currentBatchTxHashHex || "N/A"}: ${e.message}. Marked for retry.`;
      } else if (e instanceof ContractFunctionExecutionError) {
        errorMsg = `Smart contract execution error for batch ${currentBatchTxHashHex || "N/A"}: ${e.shortMessage}. Cause: ${e.cause}. Marked for retry.`;
      } else if (e instanceof ViemBaseError) {
        errorMsg = `Viem Error during batch send/build: ${e.shortMessage}. Details: ${e.details}. Marked for retry.`;
      } else {
        errorMsg = `Unexpected error during batch processing for ${currentBatchTxHashHex || "N/A"}: ${e.message}`;
      }
      console.error(errorMsg, e); 
      
      await updateBatchTransactionsInDbOptimized(
        dbClient,
        txIdsInFinalCalls,
        "failed",
        currentBatchTxHashHex,
        errorMsg,
        batchReceipt, 
      );
    } finally {
      
      const timeTaken = (Date.now() - batchProcessingStartTime) / 1000;
      const statusSummary =
        batchReceipt && batchReceipt.status === "success"
          ? "succeeded"
          : "failed/error (retries may apply)";
      console.log(
        `Batch Summary: Hash: ${currentBatchTxHashHex || "N/A"}, DB TxIDs Attempted: ${txIdsInFinalCalls.length}, On-chain Calls: ${callsForSmartWallet.length}, Status: ${statusSummary}, Time: ${timeTaken.toFixed(2)}s.`,
      );
    }
    return txIdsInFinalCalls.length;
  }
  
  /**
   * The main loop of the transaction processor.
   * Continuously fetches pending transactions, processes them in batches, and updates their statuses.
   */
  export async function mainLoop(): Promise<void> {
    let dbClient: PoolClient | null = null;
    try {
      
      dbClient = await getDbConnection(); 
    } catch (e) {
      console.error("Exiting: Initial DB connection failed on startup.");
      return; 
    }
  
    let rateCalcStartTime = Date.now();
    let dbRecordsConsideredInCycle = 0; 
  
    try {
      while (true) {
        let txCandidateBatch: TransactionRecord[] = [];
        try {
          await dbClient.query("BEGIN"); 
  
          
          const sqlQuery = `
                      SELECT
                          id, from_fid, to_fid, amount, token_address, type, status, tx_hash,
                          error_message, transaction_receipt, created_at, updated_at, retry_count
                      FROM
                          transactions
                      WHERE
                          status IN ('pending', 'failed') -- Consider both pending and failed (for retries)
                          AND retry_count <= $2 -- Ensure not permanently failed
                          -- Optional: AND created_at < NOW() - INTERVAL '1 hour' -- Example: add delay for older txs
                      ORDER BY
                          created_at ASC -- Process older transactions first
                      LIMIT $1;
                  `;
          
          
          const result = await dbClient.query<TransactionRecord>(sqlQuery, [
            FETCH_CANDIDATE_LIMIT,
            MAX_RETRIES_COUNT, 
          ]);
          txCandidateBatch = result.rows;
  
          if (txCandidateBatch.length > 0) {
            console.log(
              `Fetched ${txCandidateBatch.length} candidate transactions (pending/failed for retry).`,
            );
            dbRecordsConsideredInCycle += txCandidateBatch.length;
            
            await processBatchTransactions(dbClient, txCandidateBatch);
            await dbClient.query("COMMIT"); 
            console.log("Waiting for 60s before next batch fetch...");
            await new Promise((resolve) => setTimeout(resolve, 60000)); 
          } else {
            
            await dbClient.query("ROLLBACK"); 
            await new Promise((resolve) => setTimeout(resolve, 5000)); 
          }
        } catch (batchError: any) {
          console.error(
            `Error in processing cycle, rolling back DB changes for this cycle: ${batchError.message}`,
            batchError,
          );
          if (dbClient) {
            try {
              await dbClient.query("ROLLBACK"); 
            } catch (rbErr: any) {
              console.error("Rollback failed:", rbErr.message);
            }
          }
          console.log("Waiting for 60s after error before retrying fetch...");
          await new Promise((resolve) => setTimeout(resolve, 60000)); 
        }
  
        
        const elapsedForRateLog = (Date.now() - rateCalcStartTime) / 1000;
        if (elapsedForRateLog >= RATE_LOG_INTERVAL_SECONDS) {
          if (dbRecordsConsideredInCycle > 0 && elapsedForRateLog > 0) {
            const rate = (dbRecordsConsideredInCycle / elapsedForRateLog) * 60;
            console.log(
              `Rate: ~${rate.toFixed(2)} DB_records_considered/min over last ${elapsedForRateLog.toFixed(2)}s (${dbRecordsConsideredInCycle} records).`,
            );
          }
          dbRecordsConsideredInCycle = 0;
          rateCalcStartTime = Date.now();
        }
      }
    } catch (e_main: any) {
      console.error(
        `Critical error in main_loop: ${e_main.constructor.name} - ${e_main.message}\n${e_main.stack}`,
      );
    } finally {
      
      if (dbClient) {
        dbClient.release();
        console.log("Database client released.");
      }
      await pool.end();
      console.log("Database connection pool closed.");
    }
  }
  
  /**
   * Performs startup checks for environment variables and connectivity to RPC and Database.
   * Exits the process if any critical checks fail.
   */
  export async function startupChecks() {
    console.log("Starting TypeScript Transaction Processor...");
    const missingVars: string[] = [];
    
    const envVarsToCheck = {
      DATABASE_URL: process.env.DATABASE_URL,
      BASE_RPC_URL: process.env.BASE_RPC_URL,
      SERVER_WALLET_PRIVATE_KEY: process.env.SERVER_WALLET_PRIVATE_KEY,
      SMART_WALLET_FOR_BATCHING_ADDRESS: process.env.SMART_WALLET_FOR_BATCHING_ADDRESS,
      MULTICALL_ADDRESS_BASE_MAINNET: process.env.MULTICALL_ADDRESS,
    };
    for (const [key, value] of Object.entries(envVarsToCheck)) {
      if (!value) missingVars.push(key);
    }
    if (missingVars.length > 0) {
      console.error(
        `One or more critical .env variables missing: ${missingVars.join(", ")}. Exiting.`,
      );
      process.exit(1);
    }
  
    
    try {
      const readClientVersion = await publicClient.getBlockNumber();
      console.log(
        "Successfully connected to Base RPC (Read Client). Current block:",
        readClientVersion.toString(),
      );
    } catch (e: any) {
      console.error(
        "Error: Could not connect to Base RPC (Read Client).",
        e.message,
      );
      process.exit(1);
    }
    
    try {
      const dbTestClient = await pool.connect();
      console.log("Successfully connected to Database.");
      dbTestClient.release(); 
    } catch (e: any) {
      console.error("Error: Could not connect to Database.", e.message);
      process.exit(1);
    }
  }
  
  
  if (require.main === module) {
    startupChecks()
      .then(mainLoop)
      .catch((err) => {
        console.error(
          "Unhandled error during startup or in main loop execution:",
          err,
        );
        process.exit(1);
      });
  
    
    process.on("SIGINT", async () => {
      console.log("Caught interrupt signal, shutting down...");
      await pool.end(); 
      process.exit(0);
    });
  }
  
  