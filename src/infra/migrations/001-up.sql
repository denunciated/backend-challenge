CREATE TABLE wallets (
  id uuid PRIMARY KEY,
  player_id uuid NOT NULL,
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  balance numeric NOT NULL CHECK (balance >= 0 AND balance < power(10::numeric,36) AND scale(balance) <= 2),
  version integer NOT NULL DEFAULT 1 CHECK (version >= 1),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (player_id, currency), UNIQUE (id, currency)
);
CREATE TABLE wager_transactions (
  id uuid PRIMARY KEY,
  provider_id text NOT NULL,
  external_transaction_id text NOT NULL,
  idempotency_key text NOT NULL UNIQUE,
  payload_hash text NOT NULL CHECK (length(payload_hash) = 64),
  wallet_id uuid NOT NULL REFERENCES wallets(id),
  player_id uuid NOT NULL,
  round_id text NOT NULL,
  game_id text NOT NULL,
  kind text NOT NULL CHECK (kind IN ('OPENING','BET','WIN','LOSS','REFUND','ROLLBACK')),
  amount numeric NOT NULL CHECK (amount >= 0 AND amount < power(10::numeric,36) AND scale(amount) <= 2),
  currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
  reference_external_transaction_id text,
  reference_transaction_id uuid REFERENCES wager_transactions(id),
  status text NOT NULL CHECK (status IN ('PENDING','PENDING_REFERENCE','PROCESSED','REJECTED','FAILED')),
  failure_code text,
  result jsonb,
  reference_attempts integer NOT NULL DEFAULT 0 CHECK (reference_attempts >= 0),
  next_attempt_at timestamptz,
  correlation_id text NOT NULL,
  causation_id text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  processed_at timestamptz,
  UNIQUE (provider_id, external_transaction_id),
  UNIQUE (id, wallet_id),
  CHECK ((kind = 'LOSS' AND amount = 0) OR (kind <> 'LOSS' AND amount > 0)),
  CHECK (kind NOT IN ('REFUND','ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
  CHECK (reference_external_transaction_id IS DISTINCT FROM external_transaction_id),
  CHECK (kind NOT IN ('BET','LOSS','OPENING') OR reference_external_transaction_id IS NULL),
  CHECK ((status IN ('REJECTED','FAILED')) = (failure_code IS NOT NULL)),
  CHECK ((status IN ('PROCESSED','REJECTED','FAILED')) = (processed_at IS NOT NULL)),
  CHECK (kind <> 'OPENING' OR provider_id = 'internal'),
  CHECK (status <> 'PROCESSED' OR kind NOT IN ('REFUND','ROLLBACK') OR reference_transaction_id IS NOT NULL)
);

CREATE UNIQUE INDEX one_reversal ON wager_transactions(reference_transaction_id)
  WHERE status = 'PROCESSED' AND kind IN ('REFUND','ROLLBACK');
CREATE INDEX pending_references ON wager_transactions(next_attempt_at, id) WHERE status = 'PENDING_REFERENCE';
CREATE TABLE wallet_ledger (
  id uuid PRIMARY KEY,
  sequence bigint GENERATED ALWAYS AS IDENTITY UNIQUE,
  wallet_id uuid NOT NULL,
  transaction_id uuid NOT NULL,
  direction text NOT NULL CHECK (direction IN ('DEBIT','CREDIT')),
  amount numeric NOT NULL CHECK (amount > 0 AND amount < power(10::numeric,36) AND scale(amount) <= 2),
  currency text NOT NULL,
  balance_before numeric NOT NULL CHECK (balance_before >= 0 AND scale(balance_before) <= 2),
  balance_after numeric NOT NULL CHECK (balance_after >= 0 AND scale(balance_after) <= 2),
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  FOREIGN KEY (wallet_id,currency) REFERENCES wallets(id,currency),
  FOREIGN KEY (transaction_id,wallet_id) REFERENCES wager_transactions(id,wallet_id),
  UNIQUE (transaction_id,wallet_id),
  CHECK (balance_after = balance_before + CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END)
);
CREATE INDEX ledger_pagination ON wallet_ledger(wallet_id,sequence);
CREATE TABLE inbox (
  consumer_name text NOT NULL,
  message_id text NOT NULL,
  payload_hash text NOT NULL CHECK (length(payload_hash) = 64),
  transaction_id uuid REFERENCES wager_transactions(id),
  received_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  processed_at timestamptz,
  PRIMARY KEY (consumer_name,message_id)
);
CREATE TABLE outbox (
  id uuid PRIMARY KEY,
  aggregate_id uuid NOT NULL REFERENCES wallets(id),
  event_type text NOT NULL,
  payload jsonb NOT NULL,
  occurred_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  published_at timestamptz
);
CREATE INDEX outbox_due ON outbox(next_attempt_at,occurred_at) WHERE published_at IS NULL;

CREATE FUNCTION deny_ledger_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'ledger is append only' USING ERRCODE = '23514'; END $$;
CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION deny_ledger_mutation();
CREATE TRIGGER ledger_no_truncate BEFORE TRUNCATE ON wallet_ledger FOR EACH STATEMENT EXECUTE FUNCTION deny_ledger_mutation();

CREATE FUNCTION protect_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'transactions are audit records' USING ERRCODE='23514'; END IF;
  IF OLD.status IN ('PROCESSED','REJECTED','FAILED') THEN RAISE EXCEPTION 'terminal transaction is immutable' USING ERRCODE='23514'; END IF;
  IF (to_jsonb(NEW) - ARRAY['status','reference_transaction_id','failure_code','result','reference_attempts','next_attempt_at','processed_at'])
    IS DISTINCT FROM (to_jsonb(OLD) - ARRAY['status','reference_transaction_id','failure_code','result','reference_attempts','next_attempt_at','processed_at'])
  THEN RAISE EXCEPTION 'transaction identity is immutable' USING ERRCODE='23514'; END IF;
  IF OLD.status = 'PENDING_REFERENCE' AND NEW.status = 'PENDING' THEN RAISE EXCEPTION 'invalid transition' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER transaction_immutable BEFORE UPDATE OR DELETE ON wager_transactions FOR EACH ROW EXECUTE FUNCTION protect_transaction();

CREATE FUNCTION protect_wallet() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency THEN RAISE EXCEPTION 'wallet identity is immutable' USING ERRCODE='23514'; END IF;
  IF (NEW.balance <> OLD.balance AND NEW.version <> OLD.version + 1) OR (NEW.balance = OLD.balance AND NEW.version <> OLD.version) THEN RAISE EXCEPTION 'invalid wallet version' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER wallet_version BEFORE UPDATE ON wallets FOR EACH ROW EXECUTE FUNCTION protect_wallet();

CREATE FUNCTION check_ledger_chain() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE previous_balance numeric;
BEGIN
  PERFORM 1 FROM wallets WHERE id=NEW.wallet_id FOR UPDATE;
  SELECT balance_after INTO previous_balance FROM wallet_ledger WHERE wallet_id=NEW.wallet_id ORDER BY sequence DESC LIMIT 1;
  IF NEW.balance_before <> COALESCE(previous_balance,0) THEN RAISE EXCEPTION 'broken ledger chain' USING ERRCODE='23514'; END IF;
  RETURN NEW;
END $$;
CREATE TRIGGER ledger_chain BEFORE INSERT ON wallet_ledger FOR EACH ROW EXECUTE FUNCTION check_ledger_chain();

CREATE FUNCTION check_wallet_balance() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target uuid; stored numeric; calculated numeric;
BEGIN
  IF TG_TABLE_NAME='wallets' THEN target:=NEW.id; ELSE target:=NEW.wallet_id; END IF;
  SELECT balance INTO stored FROM wallets WHERE id=target FOR UPDATE;
  SELECT COALESCE(sum(CASE direction WHEN 'CREDIT' THEN amount ELSE -amount END),0) INTO calculated FROM wallet_ledger WHERE wallet_id=target;
  IF stored <> calculated THEN RAISE EXCEPTION 'wallet and ledger diverge' USING ERRCODE='23514'; END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER wallet_consistency AFTER INSERT OR UPDATE ON wallets DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_balance();
CREATE CONSTRAINT TRIGGER ledger_consistency AFTER INSERT ON wallet_ledger DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_wallet_balance();

CREATE FUNCTION check_transaction_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE target uuid; t wager_transactions; r wager_transactions; w wallets; e wallet_ledger; entries integer; expected_direction text;
BEGIN
  IF TG_TABLE_NAME='wager_transactions' THEN target:=NEW.id; ELSE target:=NEW.transaction_id; END IF;
  SELECT * INTO t FROM wager_transactions WHERE id=target;
  SELECT count(*) INTO entries FROM wallet_ledger WHERE transaction_id=target;
  IF t.status='PROCESSED' AND t.kind <> 'LOSS' THEN
    IF entries <> 1 THEN RAISE EXCEPTION 'processed financial transaction needs ledger' USING ERRCODE='23514'; END IF;
    SELECT * INTO e FROM wallet_ledger WHERE transaction_id=target;
    SELECT * INTO w FROM wallets WHERE id=t.wallet_id;
    expected_direction := CASE WHEN t.kind='BET' THEN 'DEBIT' ELSE 'CREDIT' END;
    IF t.kind='ROLLBACK' THEN
      SELECT * INTO r FROM wager_transactions WHERE id=t.reference_transaction_id;
      expected_direction := CASE WHEN r.kind='BET' THEN 'CREDIT' ELSE 'DEBIT' END;
    END IF;
    IF e.amount <> t.amount OR e.currency <> t.currency OR w.player_id <> t.player_id OR e.direction <> expected_direction THEN RAISE EXCEPTION 'ledger transaction mismatch' USING ERRCODE='23514'; END IF;
  ELSIF entries <> 0 THEN RAISE EXCEPTION 'nonfinancial transaction has ledger' USING ERRCODE='23514'; END IF;
  IF t.status='PROCESSED' AND t.reference_transaction_id IS NOT NULL THEN
    SELECT * INTO r FROM wager_transactions WHERE id=t.reference_transaction_id;
    IF r.status<>'PROCESSED' OR r.provider_id<>t.provider_id OR r.player_id<>t.player_id OR r.wallet_id<>t.wallet_id OR r.currency<>t.currency OR r.round_id<>t.round_id OR r.external_transaction_id<>t.reference_external_transaction_id
      OR (t.kind IN ('WIN','REFUND') AND r.kind<>'BET') OR (t.kind='ROLLBACK' AND r.kind NOT IN ('BET','WIN','REFUND'))
      OR (t.kind IN ('REFUND','ROLLBACK') AND t.amount<>r.amount)
    THEN RAISE EXCEPTION 'invalid processed reference' USING ERRCODE='23514'; END IF;
  END IF;
  RETURN NULL;
END $$;
CREATE CONSTRAINT TRIGGER transaction_ledger_consistency AFTER INSERT OR UPDATE ON wager_transactions DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_ledger();
CREATE CONSTRAINT TRIGGER ledger_transaction_consistency AFTER INSERT ON wallet_ledger DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_transaction_ledger();
