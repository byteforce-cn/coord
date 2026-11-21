package cn.byteforce.coord.sdk.pki;

/**
 * Revocation reason for {@link PkiClient#revokeCert(String, PkiRevocationReason)}.
 * <p>
 * Values are the RFC 5280 §5.3.1 CRLReason codes (7 is undefined, 8 only applies
 * to delta CRLs and is intentionally absent). The subset shared with seedj's
 * {@code PkiProvider.RevocationReason} is {@code UNSPECIFIED} / {@code KEY_COMPROMISE} /
 * {@code CA_COMPROMISE} / {@code SUPERSEDED} / {@code CESSATION_OF_OPERATION}.
 */
public enum PkiRevocationReason {
    UNSPECIFIED(0),
    KEY_COMPROMISE(1),
    CA_COMPROMISE(2),
    AFFILIATION_CHANGED(3),
    SUPERSEDED(4),
    CESSATION_OF_OPERATION(5),
    CERTIFICATE_HOLD(6),
    PRIVILEGE_WITHDRAWN(9),
    AA_COMPROMISE(10);

    private final int rfcCode;

    PkiRevocationReason(int rfcCode) {
        this.rfcCode = rfcCode;
    }

    /** RFC 5280 CRLReason numeric value (also the wire enum value). */
    public int rfcCode() {
        return rfcCode;
    }

    /**
     * Map a wire value back to the SDK enum.
     *
     * @throws IllegalArgumentException for values that are not valid revocation reasons
     */
    public static PkiRevocationReason fromRfcCode(int rfcCode) {
        for (PkiRevocationReason reason : values()) {
            if (reason.rfcCode == rfcCode) {
                return reason;
            }
        }
        throw new IllegalArgumentException("unknown RFC 5280 CRLReason: " + rfcCode);
    }
}
