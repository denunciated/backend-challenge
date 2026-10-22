export class DomainError extends Error {
  constructor(
    public readonly code: string,
    message = code,
  ) {
    super(message);
    this.name = 'DomainError';
  }
}

export class AppError extends Error {
  constructor(
    public readonly code: string,
    public readonly httpStatus: number,
  ) {
    super(code);
  }
}

export type FailureCode =
  | 'INSUFFICIENT_FUNDS'
  | 'REVERSAL_INSUFFICIENT_FUNDS'
  | 'CURRENCY_MISMATCH'
  | 'PLAYER_MISMATCH'
  | 'REFERENCE_SCOPE_MISMATCH'
  | 'REFERENCE_KIND_INVALID'
  | 'REFERENCE_NOT_PROCESSED'
  | 'ALREADY_REVERSED'
  | 'REFERENCE_EXPIRED'
  | 'AMOUNT_OVERFLOW'
  | 'INFRASTRUCTURE_PERMANENT';
