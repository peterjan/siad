package host

// TODO: seems like there would be problems with the negotiation protocols if
// the renter tried something like 'form' or 'renew' but then the connections
// dropped after the host completed the transaction but before the host was
// able to send the host signatures for the transaction.
//
// Especially on a renew, the host choosing to hold the renter signatures
// hostage could be a pretty significant problem, and would require the renter
// to attempt a double-spend to either force the transaction onto the
// blockchain or to make sure that the host cannot abscond with the funds
// without commitment.
//
// Incentive for the host to do such a thing is pretty low - they will still
// have to keep all the files following a renew in order to get the money.

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	"gitlab.com/NebulousLabs/encoding"
	"gitlab.com/NebulousLabs/errors"
	"gitlab.com/NebulousLabs/fastrand"
	connmonitor "gitlab.com/NebulousLabs/monitor"
	"gitlab.com/NebulousLabs/siamux"
	"go.sia.tech/siad/build"
	"go.sia.tech/siad/modules"
	"go.sia.tech/siad/types"
)

// defaultConnectionDeadline is the default read and write deadline which is set
// on a connection or stream. This ensures it times out if I/O exceeds this
// deadline.
const defaultConnectionDeadline = 5 * time.Minute

// rpcSettingsDeprecated is a specifier for a deprecated settings request.
var rpcSettingsDeprecated = types.NewSpecifier("Settings")

// afterCloseFn is a function that can be returned by a rpc handler. It will be
// called at the very end of the rpc after closing the stream, to make sure
// things like refunding bandwidth can accurately be handled.
type afterCloseFn func()

// threadedUpdateHostname periodically runs 'managedLearnHostname', which
// checks if the host's hostname has changed, and makes an updated host
// announcement if so.
func (h *Host) threadedUpdateHostname(closeChan chan struct{}) {
	defer close(closeChan)
	for {
		h.managedLearnHostname()
		// Wait 30 minutes to check again. If the hostname is changing
		// regularly (more than once a week), we want the host to be able to be
		// seen as having 95% uptime. Every minute that the announcement is
		// pointing to the wrong address is a minute of perceived downtime to
		// the renters.
		select {
		case <-h.tg.StopChan():
			return
		case <-time.After(time.Minute * 30):
			continue
		}
	}
}

// threadedTrackWorkingStatus periodically checks if the host is working,
// where working is defined as having received 3 settings calls in the past 15
// minutes.
func (h *Host) threadedTrackWorkingStatus(closeChan chan struct{}) {
	defer close(closeChan)

	// Before entering the longer loop, try a greedy, faster attempt to verify
	// that the host is working.
	prevSettingsCalls := atomic.LoadUint64(&h.atomicSettingsCalls)
	select {
	case <-h.tg.StopChan():
		return
	case <-time.After(workingStatusFirstCheck):
	}
	settingsCalls := atomic.LoadUint64(&h.atomicSettingsCalls)

	// sanity check
	if prevSettingsCalls > settingsCalls {
		build.Severe("the host's settings calls decremented")
	}

	h.mu.Lock()
	if settingsCalls-prevSettingsCalls >= workingStatusThreshold {
		h.workingStatus = modules.HostWorkingStatusWorking
	}
	// First check is quick, don't set to 'not working' if host has not been
	// contacted enough times.
	h.mu.Unlock()

	for {
		prevSettingsCalls = atomic.LoadUint64(&h.atomicSettingsCalls)
		select {
		case <-h.tg.StopChan():
			return
		case <-time.After(workingStatusFrequency):
		}
		settingsCalls = atomic.LoadUint64(&h.atomicSettingsCalls)

		// sanity check
		if prevSettingsCalls > settingsCalls {
			build.Severe("the host's settings calls decremented")
			continue
		}

		h.mu.Lock()
		if settingsCalls-prevSettingsCalls >= workingStatusThreshold {
			h.workingStatus = modules.HostWorkingStatusWorking
		} else {
			h.workingStatus = modules.HostWorkingStatusNotWorking
		}
		h.mu.Unlock()
	}
}

// threadedTrackConnectabilityStatus periodically checks if the host is
// connectable at its netaddress.
func (h *Host) threadedTrackConnectabilityStatus(closeChan chan struct{}) {
	defer close(closeChan)

	// Wait briefly before checking the first time. This gives time for any port
	// forwarding to complete.
	select {
	case <-h.tg.StopChan():
		return
	case <-time.After(connectabilityCheckFirstWait):
	}

	for {
		h.mu.RLock()
		autoAddr := h.autoAddress
		userAddr := h.settings.NetAddress
		h.mu.RUnlock()

		activeAddr := autoAddr
		if userAddr != "" {
			activeAddr = userAddr
		}

		dialer := &net.Dialer{
			Cancel:  h.tg.StopChan(),
			Timeout: connectabilityCheckTimeout,
		}
		conn, err := dialer.Dial("tcp", string(activeAddr))

		var status modules.HostConnectabilityStatus
		if err != nil {
			status = modules.HostConnectabilityStatusNotConnectable
		} else {
			conn.Close()
			status = modules.HostConnectabilityStatusConnectable
		}
		h.mu.Lock()
		h.connectabilityStatus = status
		h.mu.Unlock()

		select {
		case <-h.tg.StopChan():
			return
		case <-time.After(connectabilityCheckFrequency):
		}
	}
}

// initNetworking performs actions like port forwarding, and gets the
// host established on the network.
func (h *Host) initNetworking(address string) (err error) {
	// Create the listener and setup the close procedures.
	h.listener, err = h.dependencies.Listen("tcp", address)
	if err != nil {
		return err
	}
	// Automatically close the listener when h.tg.Stop() is called.
	threadedListenerClosedChan := make(chan struct{})
	h.tg.OnStop(func() {
		err := h.listener.Close()
		if err != nil {
			h.log.Println("WARN: closing the listener failed:", err)
		}

		// Wait until the threadedListener has returned to continue shutdown.
		<-threadedListenerClosedChan
	})

	// Set the initial working state of the host
	h.workingStatus = modules.HostWorkingStatusChecking

	// Set the initial connectability state of the host
	h.connectabilityStatus = modules.HostConnectabilityStatusChecking

	// Set the port.
	_, port, err := net.SplitHostPort(h.listener.Addr().String())
	if err != nil {
		return err
	}
	h.port = port
	if build.Release == "testing" {
		// Set the autoAddress to localhost for testing builds only.
		h.autoAddress = modules.NetAddress(net.JoinHostPort("localhost", h.port))
	}

	// Non-blocking, perform port forwarding and create the hostname discovery
	// thread.
	go func() {
		// Add this function to the threadgroup, so that the logger will not
		// disappear before port closing can be registered to the threadgroup
		// OnStop functions.
		err := h.tg.Add()
		if err != nil {
			// If this goroutine is not run before shutdown starts, this
			// codeblock is reachable.
			return
		}
		defer h.tg.Done()

		err = h.g.ForwardPort(port)
		if err != nil {
			h.log.Println("ERROR: failed to forward port:", err)
		}

		threadedUpdateHostnameClosedChan := make(chan struct{})
		go h.threadedUpdateHostname(threadedUpdateHostnameClosedChan)
		h.tg.OnStop(func() {
			<-threadedUpdateHostnameClosedChan
		})

		threadedTrackWorkingStatusClosedChan := make(chan struct{})
		go h.threadedTrackWorkingStatus(threadedTrackWorkingStatusClosedChan)
		h.tg.OnStop(func() {
			<-threadedTrackWorkingStatusClosedChan
		})

		threadedTrackConnectabilityStatusClosedChan := make(chan struct{})
		go h.threadedTrackConnectabilityStatus(threadedTrackConnectabilityStatusClosedChan)
		h.tg.OnStop(func() {
			<-threadedTrackConnectabilityStatusClosedChan
		})
	}()

	// Launch the listener.
	go h.threadedListen(threadedListenerClosedChan)

	// Create a listener for the SiaMux.
	if !h.dependencies.Disrupt("DisableHostSiamux") {
		err = h.staticMux.NewListener(modules.HostSiaMuxSubscriberName, h.threadedHandleStream)
		if err != nil {
			return errors.AddContext(err, "Failed to subscribe to the SiaMux")
		}
		// Close the listener when h.tg.OnStop is called.
		h.tg.OnStop(func() {
			h.staticMux.CloseListener(modules.HostSiaMuxSubscriberName)
		})
	}

	return nil
}

// threadedHandleConn handles an incoming connection to the host, typically an
// RPC.
func (h *Host) threadedHandleConn(conn net.Conn) {
	err := h.tg.Add()
	if err != nil {
		return
	}
	defer h.tg.Done()

	// Close the conn on host.Close or when the method terminates, whichever
	// comes first.
	connCloseChan := make(chan struct{})
	defer close(connCloseChan)
	go func() {
		select {
		case <-h.tg.StopChan():
		case <-connCloseChan:
		}
		conn.Close()
	}()

	// Set an initial duration that is generous, but finite. RPCs can extend
	// this if desired.
	err = conn.SetDeadline(time.Now().Add(defaultConnectionDeadline))
	if err != nil {
		h.log.Println("WARN: could not set deadline on connection:", err)
		return
	}

	// Read the first 16 bytes. If those bytes are RPCLoopEnter, then the
	// renter is attempting to use the new protocol; otherweise, assume the
	// renter is using the old protocol, and that the following 8 bytes
	// complete the renter's intended RPC ID.
	var id types.Specifier
	if err := encoding.NewDecoder(conn, encoding.DefaultAllocLimit).Decode(&id); err != nil {
		atomic.AddUint64(&h.atomicUnrecognizedCalls, 1)
		h.log.Debugf("WARN: incoming conn %v was malformed: %v", conn.RemoteAddr(), err)
		return
	}
	if id != modules.RPCLoopEnter {
		// first 8 bytes should be a length prefix of 16
		if lp := encoding.DecUint64(id[:8]); lp != 16 {
			atomic.AddUint64(&h.atomicUnrecognizedCalls, 1)
			h.log.Debugf("WARN: incoming conn %v was malformed: invalid length prefix %v", conn.RemoteAddr(), lp)
			return
		}
		// shift down 8 bytes, then read next 8
		copy(id[:8], id[8:])
		if _, err := io.ReadFull(conn, id[8:]); err != nil {
			atomic.AddUint64(&h.atomicUnrecognizedCalls, 1)
			h.log.Debugf("WARN: incoming conn %v was malformed: %v", conn.RemoteAddr(), err)
			return
		}
	}

	switch id {
	// new RPCs: enter an infinite request/response loop
	case modules.RPCLoopEnter:
		err = extendErr("incoming RPCLoopEnter failed: ", h.managedRPCLoop(conn))
	// old RPCs: handle a single request/response
	case modules.RPCDownload:
		atomic.AddUint64(&h.atomicDownloadCalls, 1)
		err = extendErr("incoming RPCDownload failed: ", h.managedRPCDownload(conn))
	case modules.RPCFormContract:
		atomic.AddUint64(&h.atomicFormContractCalls, 1)
		err = extendErr("incoming RPCFormContract failed: ", h.managedRPCFormContract(conn))
	case modules.RPCReviseContract:
		atomic.AddUint64(&h.atomicReviseCalls, 1)
		err = extendErr("incoming RPCReviseContract failed: ", h.managedRPCReviseContract(conn))
	case modules.RPCSettings:
		atomic.AddUint64(&h.atomicSettingsCalls, 1)
		err = extendErr("incoming RPCSettings failed: ", h.managedRPCSettings(conn))
	case rpcSettingsDeprecated:
		h.log.Debugln("Received deprecated settings call")
	default:
		h.log.Debugf("WARN: incoming conn %v requested unknown RPC \"%v\"", conn.RemoteAddr(), id)
		atomic.AddUint64(&h.atomicUnrecognizedCalls, 1)
	}
	if err != nil {
		atomic.AddUint64(&h.atomicErroredCalls, 1)
		err = extendErr("error with "+conn.RemoteAddr().String()+": ", err)
		h.managedLogError(err)
	}
}

// threadedHandleStream handles incoming SiaMux streams.
func (h *Host) threadedHandleStream(stream siamux.Stream) {
	var uid [8]byte
	fastrand.Read(uid[:])
	uidStr := hex.EncodeToString(uid[:])

	fmt.Println(uidStr, time.Now(), "threadedHandleStream")
	start := time.Now()
	// close the stream when the method terminates
	var cleanup afterCloseFn
	defer func() {
		if h.dependencies.Disrupt("DisableStreamClose") {
			return
		}
		err := stream.Close()
		if err != nil {
			h.log.Println("ERROR: failed to close stream:", err)
		}

		// Update used bandwidth.
		l := stream.Limit()
		atomic.AddUint64(&h.atomicStreamUpload, l.Uploaded())
		atomic.AddUint64(&h.atomicStreamDownload, l.Downloaded())

		// Call rpc specific cleanup if necessary.
		if cleanup != nil {
			cleanup()
		}
	}()

	// If the right dependency was injected we block here until it's disabled
	// again.
	for h.dependencies.Disrupt("HostBlockRPC") {
		select {
		case <-time.After(time.Second):
			continue
		case <-h.tg.StopChan():
			return
		}
	}

	err := h.tg.Add()
	if err != nil {
		return
	}
	defer h.tg.Done()

	// set an initial duration that is generous, but finite. RPCs can extend
	// this if desired
	err = stream.SetDeadline(time.Now().Add(defaultConnectionDeadline))
	if err != nil {
		h.log.Println("WARN: could not set deadline on stream:", err)
		return
	}

	// read the RPC id
	var rpcID types.Specifier
	err = modules.RPCRead(stream, &rpcID)
	if err != nil {
		err = errors.AddContext(err, "Failed to read RPC id")
		if wErr := modules.RPCWriteError(stream, err); wErr != nil {
			h.managedLogError(wErr)
		}
		atomic.AddUint64(&h.atomicUnrecognizedCalls, 1)
		return
	}

	var out string
	switch rpcID {
	case modules.RPCAccountBalance:
		fmt.Println(uidStr, time.Now(), "RPCAccountBalance")
		out, err = h.managedRPCAccountBalance(stream)
		fmt.Println(uidStr, time.Now(), "RPCAccountBalance Output:\n", out)
	case modules.RPCExecuteProgram:
		fmt.Println(uidStr, time.Now(), "RPCExecuteProgram")
		err = h.managedRPCExecuteProgram(stream)
	case modules.RPCUpdatePriceTable:
		fmt.Println(uidStr, time.Now(), "RPCUpdatePriceTable")
		out, err = h.managedRPCUpdatePriceTable(stream)
		fmt.Println(uidStr, time.Now(), "RPCUpdatePriceTable Output:\n", out)
	case modules.RPCFundAccount:
		fmt.Println(uidStr, time.Now(), "RPCFundAccount")
		err = h.managedRPCFundEphemeralAccount(stream)
	case modules.RPCLatestRevision:
		fmt.Println(uidStr, time.Now(), "RPCLatestRevision")
		err = h.managedRPCLatestRevision(stream)
	case modules.RPCRegistrySubscription:
		fmt.Println(uidStr, time.Now(), "RPCRegistrySubscription")
		cleanup, err = h.managedRPCRegistrySubscribe(stream)
	case modules.RPCRenewContract:
		fmt.Println(uidStr, time.Now(), "RPCRenewContract")
		err = h.managedRPCRenewContract(stream)
	default:
		h.log.Debugf("WARN: incoming stream %v requested unknown RPC \"%v\"", stream.RemoteAddr().String(), rpcID)
		err = errors.New(fmt.Sprintf("Unrecognized RPC id %v", rpcID))
		atomic.AddUint64(&h.atomicUnrecognizedCalls, 1)
	}

	if err != nil {
		fmt.Println(uidStr, time.Now(), "RPC error", err)
		err = errors.Compose(err, modules.RPCWriteError(stream, err))
		atomic.AddUint64(&h.atomicErroredCalls, 1)
		h.managedLogError(err)
	}
	fmt.Println(uidStr, time.Now(), string(rpcID[:]), "took", time.Since(start))
}

// threadedListen listens for incoming RPCs and spawns an appropriate handler for each.
func (h *Host) threadedListen(closeChan chan struct{}) {
	defer close(closeChan)

	// Receive connections until an error is returned by the listener. When an
	// error is returned, there will be no more calls to receive.
	for {
		// Block until there is a connection to handle.
		conn, err := h.listener.Accept()
		if err != nil {
			return
		}

		conn = connmonitor.NewMonitoredConn(conn, h.staticMonitor)

		go h.threadedHandleConn(conn)

		// Soft-sleep to ratelimit the number of incoming connections.
		select {
		case <-h.tg.StopChan():
		case <-time.After(rpcRatelimit):
		}
	}
}

// NetAddress returns the address at which the host can be reached.
func (h *Host) NetAddress() modules.NetAddress {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.settings.NetAddress != "" {
		return h.settings.NetAddress
	}
	return h.autoAddress
}

// NetworkMetrics returns information about the types of rpc calls that have
// been made to the host.
func (h *Host) NetworkMetrics() modules.HostNetworkMetrics {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return modules.HostNetworkMetrics{
		DownloadCalls:     atomic.LoadUint64(&h.atomicDownloadCalls),
		ErrorCalls:        atomic.LoadUint64(&h.atomicErroredCalls),
		FormContractCalls: atomic.LoadUint64(&h.atomicFormContractCalls),
		RenewCalls:        atomic.LoadUint64(&h.atomicRenewCalls),
		ReviseCalls:       atomic.LoadUint64(&h.atomicReviseCalls),
		SettingsCalls:     atomic.LoadUint64(&h.atomicSettingsCalls),
		UnrecognizedCalls: atomic.LoadUint64(&h.atomicUnrecognizedCalls),
	}
}
