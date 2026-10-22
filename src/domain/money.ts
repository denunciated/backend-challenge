import { DomainError } from './errors';

export interface MoneyProps {
  amount: string;
  currency: string;
}

const MAX_MINOR = 10n ** 38n - 1n;

export class Money {
  private constructor(
    private readonly minor: bigint,
    public readonly currency: string,
  ) {
    if (minor > MAX_MINOR || minor < -MAX_MINOR) throw new DomainError('AMOUNT_OVERFLOW');
    Object.freeze(this);
  }

  private static parse({ amount, currency }: MoneyProps): Money {
    if (typeof currency !== 'string' || !/^[A-Z]{3}$/.test(currency)) throw new DomainError('INVALID_CURRENCY');
    const negative = amount.startsWith('-');
    const [whole = '0', fraction = ''] = amount.replace(/^-/, '').split('.');
    const minor = BigInt(whole) * 100n + BigInt(fraction.padEnd(2, '0'));
    return new Money(negative ? -minor : minor, currency);
  }

  private assertSameCurrency(other: Money): void {
    if (this.currency !== other.currency) throw new DomainError('CURRENCY_MISMATCH');
  }

  static from(props: MoneyProps): Money {
    if (typeof props.amount !== 'string' || !/^\d+(?:\.\d{1,2})?$/.test(props.amount)) {
      throw new DomainError('INVALID_AMOUNT');
    }
    return this.parse(props);
  }

  static rehydrate(props: MoneyProps): Money {
    if (!/^-?\d+(?:\.\d{1,2})?$/.test(props.amount)) throw new DomainError('INVALID_AMOUNT');
    return this.parse(props);
  }

  static zero(currency: string): Money {
    return Money.from({ amount: '0.00', currency });
  }

  add(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minor + other.minor, this.currency);
  }

  subtract(other: Money): Money {
    this.assertSameCurrency(other);
    return new Money(this.minor - other.minor, this.currency);
  }

  negate(): Money {
    return new Money(-this.minor, this.currency);
  }

  isZero(): boolean {
    return this.minor === 0n;
  }

  isPositive(): boolean {
    return this.minor > 0n;
  }

  isNegative(): boolean {
    return this.minor < 0n;
  }

  isLessThan(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minor < other.minor;
  }

  equals(other: Money): boolean {
    this.assertSameCurrency(other);
    return this.minor === other.minor;
  }

  toJSON(): MoneyProps {
    return { amount: this.toString(), currency: this.currency };
  }

  toString(): string {
    const absolute = this.minor < 0n ? -this.minor : this.minor;
    return `${this.minor < 0n ? '-' : ''}${absolute / 100n}.${(absolute % 100n).toString().padStart(2, '0')}`;
  }
}
