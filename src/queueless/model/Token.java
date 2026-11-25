package queueless.model;

import java.time.LocalDateTime;

/**
 * A queue token. Natural ordering = queue order: priority level first,
 * then earlier issue time, then lower token id.
 */
public class Token implements Comparable<Token> {

    private final int tokenId;
    private final String tokenNumber;
    private final int customerId;
    private final ServiceType serviceType;
    private final Priority priority;
    private final LocalDateTime issuedAt;
    private TokenStatus status = TokenStatus.WAITING;
    private int counterId;               // 0 = not assigned
    private LocalDateTime calledAt;      // null until a counter calls the token

    public Token(int tokenId, String tokenNumber, int customerId,
                 ServiceType serviceType, Priority priority, LocalDateTime issuedAt) {
        this.tokenId = tokenId;
        this.tokenNumber = tokenNumber;
        this.customerId = customerId;
        this.serviceType = serviceType;
        this.priority = priority;
        this.issuedAt = issuedAt;
    }

    public int getTokenId() { return tokenId; }
    public String getTokenNumber() { return tokenNumber; }
    public int getCustomerId() { return customerId; }
    public ServiceType getServiceType() { return serviceType; }
    public Priority getPriority() { return priority; }
    public LocalDateTime getIssuedAt() { return issuedAt; }
    public TokenStatus getStatus() { return status; }
    public void setStatus(TokenStatus status) { this.status = status; }
    public int getCounterId() { return counterId; }
    public void setCounterId(int counterId) { this.counterId = counterId; }
    public LocalDateTime getCalledAt() { return calledAt; }
    public void setCalledAt(LocalDateTime calledAt) { this.calledAt = calledAt; }

    @Override
    public int compareTo(Token o) {
        int byPriority = Integer.compare(priority.getLevel(), o.priority.getLevel());
        if (byPriority != 0) return byPriority;
        int byTime = issuedAt.compareTo(o.issuedAt);
        if (byTime != 0) return byTime;
        return Integer.compare(tokenId, o.tokenId);
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof Token && ((Token) o).tokenId == tokenId;
    }

    @Override
    public int hashCode() { return Integer.hashCode(tokenId); }

    @Override
    public String toString() {
        return tokenNumber + " [" + priority + ", " + status + "]";
    }
}
