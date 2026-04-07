public class Transaction {
    private final long transactionId;
    private final long userId;
    private final double amount;
    private final String currency;
    private final String category;
    private final boolean premiumUser;

    private final double convertedAmountUah;
    private final double cashbackAmountUah;
    private final double finalAmountUah;

    private final boolean poisonPill;

    public Transaction(
            long transactionId,
            long userId,
            double amount,
            String currency,
            String category,
            boolean premiumUser
    ) {
        this(
                transactionId,
                userId,
                amount,
                currency,
                category,
                premiumUser,
                0.0,
                0.0,
                0.0,
                false
        );
    }

    private Transaction(
            long transactionId,
            long userId,
            double amount,
            String currency,
            String category,
            boolean premiumUser,
            double convertedAmountUah,
            double cashbackAmountUah,
            double finalAmountUah,
            boolean poisonPill
    ) {
        this.transactionId = transactionId;
        this.userId = userId;
        this.amount = amount;
        this.currency = currency;
        this.category = category;
        this.premiumUser = premiumUser;
        this.convertedAmountUah = convertedAmountUah;
        this.cashbackAmountUah = cashbackAmountUah;
        this.finalAmountUah = finalAmountUah;
        this.poisonPill = poisonPill;
    }

    public static Transaction poisonPill() {
        return new Transaction(
                -1L,
                -1L,
                0.0,
                "POISON",
                "POISON",
                false,
                0.0,
                0.0,
                0.0,
                true
        );
    }

    public Transaction withConvertedAmountUah(double convertedAmountUah) {
        return new Transaction(
                transactionId,
                userId,
                amount,
                currency,
                category,
                premiumUser,
                convertedAmountUah,
                0.0,
                convertedAmountUah,
                false
        );
    }

    public Transaction withCashbackAmountUah(double cashbackAmountUah) {
        double finalAmount = Math.round((convertedAmountUah - cashbackAmountUah) * 100.0) / 100.0;

        return new Transaction(
                transactionId,
                userId,
                amount,
                currency,
                category,
                premiumUser,
                convertedAmountUah,
                cashbackAmountUah,
                finalAmount,
                false
        );
    }

    public long getTransactionId() {
        return transactionId;
    }

    public long getUserId() {
        return userId;
    }

    public double getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getCategory() {
        return category;
    }

    public boolean isPremiumUser() {
        return premiumUser;
    }

    public double getConvertedAmountUah() {
        return convertedAmountUah;
    }

    public double getCashbackAmountUah() {
        return cashbackAmountUah;
    }

    public double getFinalAmountUah() {
        return finalAmountUah;
    }

    public boolean isPoisonPill() {
        return poisonPill;
    }

    @Override
    public String toString() {
        return "Transaction{" +
                "transactionId=" + transactionId +
                ", userId=" + userId +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", category='" + category + '\'' +
                ", premiumUser=" + premiumUser +
                ", convertedAmountUah=" + convertedAmountUah +
                ", cashbackAmountUah=" + cashbackAmountUah +
                ", finalAmountUah=" + finalAmountUah +
                ", poisonPill=" + poisonPill +
                '}';
    }
}
