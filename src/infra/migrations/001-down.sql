DROP TABLE outbox, inbox, wallet_ledger, wager_transactions, wallets CASCADE;
DROP FUNCTION check_transaction_ledger(), check_wallet_balance(), check_ledger_chain(), protect_wallet(), protect_transaction(), deny_ledger_mutation();
