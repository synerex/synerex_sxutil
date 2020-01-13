package sxutil // import "github.com/synerex/synerex_sxutil"

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/bwmarrin/snowflake"
	"github.com/golang/protobuf/ptypes"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
	api "github.com/synerex/synerex_api"
	nodeapi "github.com/synerex/synerex_nodeapi"
	pbase "github.com/synerex/synerex_proto"
	"google.golang.org/grpc"
)

// sxutil.go is a helper utility package for Synerex

// Helper structures for Synerex

// IDType for all ID in Synerex
type IDType uint64

/* remove Global variables 2020/01
var (
	node         *snowflake.Node // package variable for keeping unique ID.
	nid          *nodeapi.NodeID
	nupd         *nodeapi.NodeUpdate
	numu         sync.RWMutex
	myNodeName   string
	myServerInfo = ""
	myNodeType   nodeapi.NodeType
	conn         *grpc.ClientConn
	clt          nodeapi.NodeClient
	msgCount     uint64
	nodeState    = NewNodeState()
)
*/

const WAIT_TIME = 30

// NodeservInfo is a connection info for each Node Server
type NodeServInfo struct { // we keep this for each nodeserver.
	node         *snowflake.Node // package variable for keeping unique ID.
	nid          *nodeapi.NodeID
	nupd         *nodeapi.NodeUpdate
	numu         sync.RWMutex
	myNodeName   string
	myServerInfo string
	myNodeType   nodeapi.NodeType
	conn         *grpc.ClientConn
	clt          nodeapi.NodeClient
	msgCount     uint64
	nodeState    *NodeState
}

var defaultNI *NodeServInfo

// DemandOpts is sender options for Demand
type DemandOpts struct {
	ID     uint64
	Target uint64
	Name   string
	JSON   string
	Cdata  *api.Content
}

// SupplyOpts is sender options for Supply
type SupplyOpts struct {
	ID     uint64
	Target uint64
	Name   string
	JSON   string
	Cdata  *api.Content
}

type SxServerOpt struct {
	NodeType   nodeapi.NodeType
	ServerInfo string
	ClusterId  int32
	AreaId     string
	GwInfo     string
}

type NodeState struct {
	ProposedSupply []api.Supply
	ProposedDemand []api.Demand
	Locked         bool
}

func NewNodeState() *NodeState {
	obj := new(NodeState)
	obj.init()

	log.Println("Initializing NodeState")
	return obj
}

func (ns NodeState) init() {
	ns.ProposedSupply = []api.Supply{}
	ns.ProposedDemand = []api.Demand{}
	ns.Locked = false
}

func (ns NodeState) isSafeState() bool {
	log.Printf("NodeState#isSafeState is called[%v]", ns)
	return len(ns.ProposedSupply) == 0 && len(ns.ProposedDemand) == 0
}

func (ns NodeState) proposeSupply(supply api.Supply) {
	log.Printf("NodeState#proposeSupply[%d] is called", supply.Id)
	ns.ProposedSupply = append(ns.ProposedSupply, supply)
}

func (ns NodeState) selectSupply(id uint64) bool {
	log.Printf("NodeState#selectSupply[%d] is called\n", id)

	pos := -1
	for i := 0; i < len(ns.ProposedSupply); i++ {
		if ns.ProposedSupply[i].Id == id {
			pos = i
		}
	}

	if pos >= 0 {
		ns.ProposedSupply = append(ns.ProposedSupply[:pos], ns.ProposedSupply[pos+1:]...)
		return true
	} else {
		log.Printf("not found supply[%d]\n", id)

		return false
	}
}

func (ns NodeState) proposeDemand(demand api.Demand) {
	log.Printf("NodeState#proposeDemand[%d] is called\n", demand.Id)
	ns.ProposedDemand = append(ns.ProposedDemand, demand)
}

func (ns NodeState) selectDemand(id uint64) bool {
	log.Printf("NodeState#selectDemand[%d] is called\n", id)

	pos := -1
	for i := 0; i < len(ns.ProposedDemand); i++ {
		if ns.ProposedDemand[i].Id == id {
			pos = i
		}
	}

	if pos >= 0 {
		ns.ProposedDemand = append(ns.ProposedDemand[:pos], ns.ProposedDemand[pos+1:]...)
		return true
	} else {
		log.Printf("not found supply[%d]\n", id)

		return false
	}
}

func init() {
	fmt.Println("Synergic Exchange Util init() is called!")
	defaultNI = NewNodeServInfo()
}

// GetDefaultNodeServInfo returns Default NodeServ Info for sxutil
func GetDefaultNodeServInfo() *NodeServInfo{
	return defaultNI
}

// NewNodeServInfo returns new NodeServ Info for sxutil
func NewNodeServInfo() *NodeServInfo {
	return &NodeServInfo{
		nodeState: NewNodeState(),
	}
}


// InitNodeNum for initialize NodeNum again
func InitNodeNum(n int) {
	var err error
	defaultNI.node, err = snowflake.NewNode(int64(n))
	if err != nil {
		log.Println("Error in initializing snowflake:", err)
	} else {
		log.Println("Successfully Initialize node ", n)
	}
}

// GetNodeName returns node name from node_id
func (ni *NodeServInfo) GetNodeName(n int) string {
	nid, err := ni.clt.QueryNode(context.Background(), &nodeapi.NodeID{NodeId: int32(n)})
	if err != nil {
		log.Printf("Error on QueryNode %v", err)
		return "Unknown"
	}
	return nid.NodeName
}

/*
func GetNodeName(n int) string {
	ni, err := defaultNI.clt.QueryNode(context.Background(), &nodeapi.NodeID{NodeId: int32(n)})
	if err != nil {
		log.Printf("Error on QueryNode %v", err)
		return "Unknown"
	}
	return ni.NodeName
}
*/

// SetNodeStatus updates KeepAlive info to NodeServer
func (ni *NodeServInfo) SetNodeStatus(status int32, arg string) {
	ni.numu.Lock()
	ni.nupd.NodeStatus = status
	ni.nupd.NodeArg = arg
	ni.numu.Unlock()
}

// SetNodeStatus updates KeepAlive info to NodeServer
func SetNodeStatus(status int32, arg string) {
	defaultNI.SetNodeStatus(status,arg)
}


func (ni *NodeServInfo )reconnectNodeServ() error { // re_send connection info to server.
	nif := nodeapi.NodeInfo{
		NodeName:         ni.myNodeName,
		NodeType:         ni.myNodeType,
		ServerInfo:       ni.myServerInfo,             // TODO: this is not correctly initialized
		NodePbaseVersion: pbase.ChannelTypeVersion, // this is defined at compile time
		WithNodeId:       ni.nid.NodeId,
	}
	var ee error
	ni.nid, ee = ni.clt.RegisterNode(context.Background(), &nif)
	if ee != nil { // has error!
		log.Println("Error on get NodeID", ee)
		return ee
	} else {
		var nderr error
		ni.node, nderr = snowflake.NewNode(int64(ni.nid.NodeId))
		if nderr != nil {
			log.Println("Error in initializing snowflake:", nderr)
			return nderr
		} else {
			log.Println("Successfully ReInitialize node ", ni.nid.NodeId)
		}
	}

	ni.nupd = &nodeapi.NodeUpdate{
		NodeId:      ni.nid.NodeId,
		Secret:      ni.nid.Secret,
		UpdateCount: 0,
		NodeStatus:  0,
		NodeArg:     "",
	}
	//	fmt.Println("KeepAlive started!")
	return nil
}

// for simple keepalive
//func startKeepAlive() {/
//	defaultNI.startKeepAliveWithCmd(nil)
//}

func (ni *NodeServInfo) startKeepAliveWithCmd(cmd_func func(nodeapi.KeepAliveCommand, string)) {
	for {
		ni.msgCount = 0 // how count message?
		//		fmt.Printf("KeepAlive %s %d\n",nupd.NodeStatus, nid.KeepaliveDuration)
		time.Sleep(time.Second * time.Duration(ni.nid.KeepaliveDuration))
		if ni.nid.Secret == 0 { // this means the node is disconnected
			break
		}

		if ni.myNodeType == nodeapi.NodeType_SERVER {
			c, _ := cpu.Percent(5, false)
			v, _ := mem.VirtualMemory()
			var status nodeapi.ServerStatus
			status = nodeapi.ServerStatus{
				Cpu:      c[0],
				Memory:   v.UsedPercent,
				MsgCount: ni.msgCount,
			}
			ni.nupd.Status = &status
		}

		ni.numu.RLock()
		ni.nupd.UpdateCount++
		resp, err := ni.clt.KeepAlive(context.Background(), ni.nupd)
		ni.numu.RUnlock()
		if err != nil {
			log.Printf("Error in response, may nodeserv failuer %v:%v", resp, err)
		}
		if resp != nil  { // there might be some errors in response
			switch resp.Command {
			case nodeapi.KeepAliveCommand_RECONNECT: // order is reconnect to node.
				ni.reconnectNodeServ()
			case nodeapi.KeepAliveCommand_SERVER_CHANGE :
				log.Printf("receive SERVER_CHANGE\n")

				if ni.nodeState.isSafeState() {
					ni.UnRegisterNode()

					if ni.conn != nil {
						ni.conn.Close()
					}

					if cmd_func != nil {
						cmd_func(resp.Command, resp.Err)
						ni.nodeState.init()
					}
				} else {
					// wait
					if !ni.nodeState.Locked {
						ni.nodeState.Locked = true
						go func() {
							t := time.NewTicker(WAIT_TIME * time.Second) // 30 seconds
							<-t.C
							ni.nodeState.init()
							t.Stop() // タイマを止める。
						}()
					}
				}
			case nodeapi.KeepAliveCommand_PROVIDER_DISCONNECT:
				log.Printf("receive PROV_DISCONN %s\n",  resp.Err)
				if ni.myNodeType != nodeapi.NodeType_SERVER {
					log.Printf("NodeType shoud be SERVER! %d %s %#v", ni.myNodeType, ni.myNodeName, resp)
				} else if cmd_func != nil {
					// work provider disconnect
					cmd_func(resp.Command, resp.Err)
				}
			}
		}
	}
}

func (ni *NodeServInfo) MsgCountUp() {
	ni.msgCount++
}


func MsgCountUp() { // is this needed?
	defaultNI.MsgCountUp()
}


// RegisterNode is a function to register Node with node server address
func RegisterNode(nodesrv string, nm string, channels []uint32, serv *SxServerOpt) (string, error) { // register ID to server
	return RegisterNodeWithCmd(nodesrv, nm, channels, serv, nil)
}

// RegisterNodeWithCmd is a function to register Node with node server address and KeepAlive Command Callback
func RegisterNodeWithCmd(nodesrv string, nm string, channels []uint32, serv *SxServerOpt, cmd_func func(nodeapi.KeepAliveCommand,string)) (string, error) { // register ID to server
	return defaultNI.RegisterNodeWithCmd(nodesrv, nm, channels, serv, cmd_func)
}

// RegisterNodeWithCmd is a function to register Node with node server address and KeepAlive Command Callback
func (ni *NodeServInfo) RegisterNodeWithCmd(nodesrv string, nm string, channels []uint32, serv *SxServerOpt, cmd_func func(nodeapi.KeepAliveCommand,string)) (string, error) { // register ID to server
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure()) // insecure
	var err error
	ni.conn, err = grpc.Dial(nodesrv, opts...)
	if err != nil {
		log.Printf("fail to dial: %v", err)
		return "", err
	}
	//	defer conn.Close()

	ni.clt = nodeapi.NewNodeClient(ni.conn)
	var nif nodeapi.NodeInfo
	var nodeId int32

	if ni.nid == nil {
		nodeId = -1 // initial registration
	} else {
		nodeId = ni.nid.NodeId
	}

	if serv == nil {
		ni.myNodeType = nodeapi.NodeType_PROVIDER
		nif = nodeapi.NodeInfo{
			NodeName:         nm,
			NodeType:         ni.myNodeType,
			ServerInfo:       "",
			NodePbaseVersion: pbase.ChannelTypeVersion, // this is defined at compile time
			WithNodeId:       nodeId,
			ClusterId:        0,         // default cluster
			AreaId:           "Default", //default area
			ChannelTypes:     channels,  // channel types
		}
	} else {
		ni.myNodeType = serv.NodeType
		ni.myServerInfo = serv.ServerInfo
		nif = nodeapi.NodeInfo{
			NodeName:         nm,
			NodeType:         ni.myNodeType,
			ServerInfo:       ni.myServerInfo,
			NodePbaseVersion: pbase.ChannelTypeVersion, // this is defined at compile time
			WithNodeId:       nodeId,
			ClusterId:        serv.ClusterId, // default cluster
			AreaId:           serv.AreaId,    //default area
			ChannelTypes:     channels,       // channel types
			GwInfo:           serv.GwInfo,
		}
	}
	ni.myNodeName = nm
	var ee error
	ni.nid, ee = ni.clt.RegisterNode(context.Background(), &nif)
	if ee != nil { // has error!
		log.Println("Error on get NodeID", ee)
		return "", ee
	} else {
		var nderr error
		ni.node, nderr = snowflake.NewNode(int64(ni.nid.NodeId))
		if nderr != nil {
			log.Println("Error in initializing snowflake:", err)
			return "", nderr
		} else {
			log.Println("Successfully ReInitialize node ", ni.nid.NodeId)
		}
	}
	ni.nupd = &nodeapi.NodeUpdate{
		NodeId:      ni.nid.NodeId,
		Secret:      ni.nid.Secret,
		UpdateCount: 0,
		NodeStatus:  0,
		NodeArg:     "",
	}
	// start keepalive goroutine
	go ni.startKeepAliveWithCmd(cmd_func)
	//	fmt.Println("KeepAlive started!")
	return ni.nid.ServerInfo, nil
}

// UnRegisterNode de-registrate node id
func UnRegisterNode() {
	defaultNI.UnRegisterNode()
}


// UnRegisterNode de-registrate node id
func (ni *NodeServInfo) UnRegisterNode() {
	log.Println("UnRegister Node ", ni.nid)
	resp, err := ni.clt.UnRegisterNode(context.Background(), ni.nid)
	ni.nid.Secret = 0
	if err != nil || !resp.Ok {
		log.Print("Can't unregister", err, resp)
	}
}

// SXServiceClient Wrappter Structure for market client
type SXServiceClient struct {
	ClientID    IDType
	ChannelType uint32
	Client      api.SynerexClient
	ArgJson     string
	MbusID      IDType
	NI          *NodeServInfo
}

// GrpcConnectServer is a utility function for conneting gRPC server
func GrpcConnectServer(serverAddress string) api.SynerexClient { // TODO: we may add connection option
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure()) // currently we do not use sercure connection //TODO: we need to udpate SSL
	conn, err := grpc.Dial(serverAddress, opts...)
	if err != nil {
		log.Printf("fail to connect server %s: %v", serverAddress, err)
		return nil
	}
	return api.NewSynerexClient(conn)
}

// NewSXServiceClient Creates wrapper structre SXServiceClient from SynerexClient
func NewSXServiceClient(clt api.SynerexClient, mtype uint32, argJson string) *SXServiceClient {
	return defaultNI.NewSXServiceClient(clt, mtype, argJson)
}

// NewSXServiceClient Creates wrapper structre SXServiceClient from SynerexClient
func (ni *NodeServInfo) NewSXServiceClient(clt api.SynerexClient, mtype uint32, argJson string) *SXServiceClient {
	s := &SXServiceClient{
		ClientID:    IDType(ni.node.Generate()),
		ChannelType: mtype,
		Client:      clt,
		ArgJson:     argJson,
		NI: ni,
	}
	return s
}

// GenerateIntID for generate uniquie ID
func GenerateIntID() uint64 {
	return defaultNI.GenerateIntID()
}

// GenerateIntID for generate uniquie ID
func (ni *NodeServInfo)GenerateIntID() uint64 {
	return uint64(ni.node.Generate())
}

func (clt SXServiceClient) getChannel() *api.Channel {
	return &api.Channel{ClientId: uint64(clt.ClientID), ChannelType: clt.ChannelType, ArgJson: clt.ArgJson}
}

// IsSupplyTarget is a helper function to check target
func (clt *SXServiceClient) IsSupplyTarget(sp *api.Supply, idlist []uint64) bool {
	spid := sp.TargetId
	for _, id := range idlist {
		if id == spid {
			return true
		}
	}
	return false
}

// IsDemandTarget is a helper function to check target
func (clt *SXServiceClient) IsDemandTarget(dm *api.Demand, idlist []uint64) bool {
	dmid := dm.TargetId
	for _, id := range idlist {
		if id == dmid {
			return true
		}
	}
	return false
}

// ProposeSupply send proposal Supply message to server
func (clt *SXServiceClient) ProposeSupply(spo *SupplyOpts) uint64 {
	pid := GenerateIntID()
	sp := &api.Supply{
		Id:          pid,
		SenderId:    uint64(clt.ClientID),
		TargetId:    spo.Target,
		ChannelType: clt.ChannelType,
		SupplyName:  spo.Name,
		Ts:          ptypes.TimestampNow(),
		ArgJson:     spo.JSON,
		Cdata:       spo.Cdata,
	}

	//	switch clt.ChannelType {//
	//Todo: We need to make if for each channel type
	//	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := clt.Client.ProposeSupply(ctx, sp)
	if err != nil {
		log.Printf("%v.ProposeSupply err %v, [%v]", clt, err, sp)
		return 0 // should check...
	}
	//	log.Println("ProposeSupply Response:", resp, ":PID ",pid)

	clt.NI.nodeState.proposeSupply(*sp)

	return pid
}

// SelectSupply send select message to server
func (clt *SXServiceClient) SelectSupply(sp *api.Supply) (uint64, error) {
	tgt := &api.Target{
		Id:          GenerateIntID(),
		SenderId:    uint64(clt.ClientID),
		TargetId:    sp.Id, /// Message Id of Supply (not SenderId),
		ChannelType: sp.ChannelType,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := clt.Client.SelectSupply(ctx, tgt)
	if err != nil {
		log.Printf("%v.SelectSupply err %v %v", clt, err, resp)
		return 0, err
	}
	//	log.Println("SelectSupply Response:", resp)
	// if mbus is OK, start mbus!
	clt.MbusID = IDType(resp.MbusId)
	if clt.MbusID != 0 {
		//TODO:  We need to implement Mbus systems
		//		clt.SubscribeMbus()
	}

	clt.NI.nodeState.selectSupply(sp.Id)

	return uint64(clt.MbusID), nil
}

// SelectDemand send select message to server
func (clt *SXServiceClient) SelectDemand(dm *api.Demand) error {
	tgt := &api.Target{
		Id:          GenerateIntID(),
		SenderId:    uint64(clt.ClientID),
		TargetId:    dm.Id,
		ChannelType: dm.ChannelType,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := clt.Client.SelectDemand(ctx, tgt)
	if err != nil {
		log.Printf("%v.SelectDemand err %v %v", clt, err, resp)
		return err
	}
	//	log.Println("SelectDemand Response:", resp)

	clt.NI.nodeState.selectDemand(dm.Id)

	return nil
}

// SubscribeSupply  Wrapper function for SXServiceClient
func (clt *SXServiceClient) SubscribeSupply(ctx context.Context, spcb func(*SXServiceClient, *api.Supply)) error {
	ch := clt.getChannel()
	smc, err := clt.Client.SubscribeSupply(ctx, ch)
	if err != nil {
		log.Printf("%v SubscribeSupply Error %v", clt, err)
		return err
	}
	for {
		var sp *api.Supply
		sp, err = smc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Supply subscribe OK")
			} else {
				log.Printf("%v SXServiceClient SubscribeSupply error [%v]", clt, err)
			}
			break
		}
		//		log.Println("Receive SS:", *sp)

		if !clt.NI.nodeState.Locked {
			spcb(clt, sp)
		} else {
			log.Println("Provider is locked!")
		}
	}
	return err
}

// SubscribeDemand  Wrapper function for SXServiceClient
func (clt *SXServiceClient) SubscribeDemand(ctx context.Context, dmcb func(*SXServiceClient, *api.Demand)) error {
	ch := clt.getChannel()
	dmc, err := clt.Client.SubscribeDemand(ctx, ch)
	if err != nil {
		log.Printf("%v SubscribeDemand Error %v", clt, err)
		return err // sender should handle error...
	}
	for {
		var dm *api.Demand
		dm, err = dmc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Demand subscribe OK")
			} else {
				log.Printf("%v SXServiceClient SubscribeDemand error [%v]", clt, err)
			}
			break
		}
		//	log.Println("Receive SD:",*dm)

		// call Callback!
		if !clt.NI.nodeState.Locked {
			dmcb(clt, dm)
		} else {
			log.Println("Provider is locked!")
		}
	}
	return err
}

// SubscribeMbus  Wrapper function for SXServiceClient
func (clt *SXServiceClient) SubscribeMbus(ctx context.Context, mbcb func(*SXServiceClient, *api.MbusMsg)) error {

	mb := &api.Mbus{
		ClientId: uint64(clt.ClientID),
		MbusId:   uint64(clt.MbusID),
	}

	smc, err := clt.Client.SubscribeMbus(ctx, mb)
	if err != nil {
		log.Printf("%v Synerex_SubscribeMbusClient Error %v", clt, err)
		return err // sender should handle error...
	}
	for {
		var mes *api.MbusMsg
		mes, err = smc.Recv() // receive Demand
		if err != nil {
			if err == io.EOF {
				log.Print("End Mbus subscribe OK")
			} else {
				log.Printf("%v SXServiceClient SubscribeMbus error %v", clt, err)
			}
			break
		}
		//		log.Printf("Receive Mbus Message %v", *mes)
		// call Callback!
		mbcb(clt, mes)
	}
	return err
}

func (clt *SXServiceClient) SendMsg(ctx context.Context, msg *api.MbusMsg) error {
	if clt.MbusID == 0 {
		return errors.New("No Mbus opened!")
	}
	msg.MsgId = GenerateIntID()
	msg.SenderId = uint64(clt.ClientID)
	msg.MbusId = uint64(clt.MbusID)
	_, err := clt.Client.SendMsg(ctx, msg)

	return err
}

func (clt *SXServiceClient) CloseMbus(ctx context.Context) error {
	if clt.MbusID == 0 {
		return errors.New("No Mbus opened!")
	}
	mbus := &api.Mbus{
		ClientId: uint64(clt.ClientID),
		MbusId:   uint64(clt.MbusID),
	}
	_, err := clt.Client.CloseMbus(ctx, mbus)
	if err == nil {
		clt.MbusID = 0
	}
	return err
}

// NotifyDemand sends Typed Demand to Server
func (clt *SXServiceClient) NotifyDemand(dmo *DemandOpts) (uint64, error) {
	id := GenerateIntID()
	ts := ptypes.TimestampNow()
	dm := api.Demand{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: clt.ChannelType,
		DemandName:  dmo.Name,
		Ts:          ts,
		ArgJson:     dmo.JSON,
		Cdata:       dmo.Cdata,
	}
	//	switch clt.ChannelType {
	//	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := clt.Client.NotifyDemand(ctx, &dm)

	//	resp, err := clt.Client.NotifyDemand(ctx, &dm)
	if err != nil {
		log.Printf("%v.NotifyDemand err %v", clt, err)
		return 0, err
	}
	//	log.Println(resp)
	dmo.ID = id // assign ID
	return id, nil
}

// NotifySupply sends Typed Supply to Server
func (clt *SXServiceClient) NotifySupply(smo *SupplyOpts) (uint64, error) {
	id := GenerateIntID()
	ts := ptypes.TimestampNow()
	dm := api.Supply{
		Id:          id,
		SenderId:    uint64(clt.ClientID),
		ChannelType: clt.ChannelType,
		SupplyName:  smo.Name,
		Ts:          ts,
		ArgJson:     smo.JSON,
		Cdata:       smo.Cdata,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	//	resp , err := clt.Client.NotifySupply(ctx, &dm)

	_, err := clt.Client.NotifySupply(ctx, &dm)
	if err != nil {
		log.Printf("Error for sending:NotifySupply to  Synerex Server as %v ", err)
		return 0, err
	}
	//	log.Println("RegiterSupply:", smo, resp)
	smo.ID = id // assign ID
	return id, nil
}

// Confirm sends confirm message to sender
func (clt *SXServiceClient) Confirm(id IDType) error {
	tg := &api.Target{
		Id:          GenerateIntID(),
		SenderId:    uint64(clt.ClientID),
		TargetId:    uint64(id),
		ChannelType: clt.ChannelType,
		MbusId:      uint64(id),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := clt.Client.Confirm(ctx, tg)
	if err != nil {
		log.Printf("%v Confirm Failier %v %v", clt, err, resp)
		return err
	}
	clt.MbusID = id
	//	log.Println("Confirm Success:", resp)

	clt.NI.nodeState.selectDemand(uint64(id))
	clt.NI.nodeState.selectSupply(uint64(id))

	return nil
}
