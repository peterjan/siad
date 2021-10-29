package host

import (
	"fmt"
	"time"

	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/siamux"
	"go.sia.tech/siad/modules"
)

// managedRPCAccountBalance handles the RPC which returns the balance of the
// requested account.
// TODO: Should we require a signature for retrieving the balance?
func (h *Host) managedRPCAccountBalance(stream siamux.Stream) (string, error) {
	out := ""
	start := time.Now()
	// read the price table
	pt, err := h.staticReadPriceTableID(stream)
	if err != nil {
		return out, errors.AddContext(err, "failed to read price table")
	}
	out += fmt.Sprintf("PT read took %v\n", time.Since(start))

	// Process payment.
	start = time.Now()
	pd, err := h.ProcessPayment(stream, pt.HostBlockHeight)
	if err != nil {
		return out, errors.AddContext(err, "failed to process payment")
	}
	out += fmt.Sprintf("process payment took %v\n", time.Since(start))

	// Check payment.
	if pd.Amount().Cmp(pt.AccountBalanceCost) < 0 {
		return out, modules.ErrInsufficientPaymentForRPC
	}

	// Refund excessive payment.
	start = time.Now()
	refund := pd.Amount().Sub(pt.AccountBalanceCost)
	err = h.staticAccountManager.callRefund(pd.AccountID(), refund)
	if err != nil {
		return out, errors.AddContext(err, "failed to refund client")
	}
	out += fmt.Sprintf("refund took %v\n", time.Since(start))

	// Read request
	start = time.Now()
	var abr modules.AccountBalanceRequest
	err = modules.RPCRead(stream, &abr)
	if err != nil {
		return out, errors.AddContext(err, "Failed to read AccountBalanceRequest")
	}
	out += fmt.Sprintf("REQ read took %v\n", time.Since(start))

	// Get account balance.
	start = time.Now()
	balance := h.staticAccountManager.callAccountBalance(abr.Account)
	out += fmt.Sprintf("account balance took %v\n", time.Since(start))

	start = time.Now()
	// Send response.
	err = modules.RPCWrite(stream, modules.AccountBalanceResponse{
		Balance: balance,
	})
	if err != nil {
		return out, errors.AddContext(err, "Failed to send AccountBalanceResponse")
	}
	out += fmt.Sprintf("send took %v\n", time.Since(start))
	return out, nil
}
