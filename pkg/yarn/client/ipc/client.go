/*
Copyright 2013 The Cloudera Inc.
Copyright 2023 The Koordinator Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ipc

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	gouuid "github.com/nu7hatch/gouuid"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"k8s.io/klog/v2"

	yarnauth "github.com/koordinator-sh/yarn-copilot/pkg/yarn/apis/auth"
	hadoop_common "github.com/koordinator-sh/yarn-copilot/pkg/yarn/apis/proto/hadoopcommon"
	"github.com/koordinator-sh/yarn-copilot/pkg/yarn/apis/security"
)

const (
	connDefaultTimeout = 10 * time.Second
	rwDefaultTimeout   = 5 * time.Second
)

type Client struct {
	ClientId      *gouuid.UUID
	Ugi           *hadoop_common.UserInformationProto
	ServerAddress string
	TCPNoDelay    bool
}

type connection struct {
	con *net.TCPConn
}

type connection_id struct {
	user     string
	protocol string
	address  string
	ClientId gouuid.UUID
}

type call struct {
	callId    int32
	procedure proto.Message
	request   proto.Message
	response  proto.Message
	// err        *error
	retryCount int32
}

func (c *Client) String() string {
	buf := bytes.NewBufferString("")
	fmt.Fprint(buf, "<clientId:", c.ClientId)
	fmt.Fprint(buf, ", server:", c.ServerAddress)
	fmt.Fprint(buf, ">")
	return buf.String()
}

var (
	SASL_RPC_DUMMY_CLIENT_ID     []byte = make([]byte, 0)
	SASL_RPC_CALL_ID             int32  = -33
	SASL_RPC_INVALID_RETRY_COUNT int32  = -1
)

func (c *Client) Call(rpc *hadoop_common.RequestHeaderProto, rpcRequest proto.Message, rpcResponse proto.Message) error {
	// Create connection_id
	connectionId := connection_id{
		user:     *c.Ugi.RealUser,
		protocol: *rpc.DeclaringClassProtocolName,
		address:  c.ServerAddress,
		ClientId: *c.ClientId,
	}

	// Get connection to server
	klog.V(5).Infof("Connecting... %v", c)
	conn, err := getConnection(c, &connectionId)
	if err != nil {
		return err
	}

	// Create call and send request
	rpcCall := call{callId: 0, procedure: rpc, request: rpcRequest, response: rpcResponse}
	err = sendRequest(c, conn, &rpcCall)
	if err != nil {
		klog.Warningf("sendRequest", err)
		return err
	}

	// Read & return response
	err = c.readResponse(conn, &rpcCall)

	// TODO keep connection alive for reuse
	conn.con.Close()

	return err
}

//var connectionPool = struct {
//	sync.RWMutex
//	connections map[connection_id]*connection
//}{connections: make(map[connection_id]*connection)}

func findUsableTokenForService(service string) (*hadoop_common.TokenProto, bool) {
	userTokens := security.GetCurrentUser().GetUserTokens()

	klog.V(5).Infof("looking for token for service: %s\n", service)

	if len(userTokens) == 0 {
		return nil, false
	}

	token := userTokens[service]
	if token != nil {
		return token, true
	}

	return nil, false
}

func getConnection(c *Client, connectionId *connection_id) (*connection, error) {
	// Try to re-use an existing connection
	//connectionPool.RLock()
	//con := connectionPool.connections[*connectionId]
	//connectionPool.RUnlock()

	// If necessary, create a new connection and save it in the connection-pool
	//var err error
	//if con == nil {
	con, err := setupConnection(c)
	if err != nil {
		klog.Warningf("Couldn't setup connection: %v", err)
		return nil, err
	}

	//connectionPool.Lock()
	//connectionPool.connections[*connectionId] = con
	//connectionPool.Unlock()

	var authProtocol yarnauth.AuthProtocol = yarnauth.AUTH_PROTOCOL_NONE

	if _, found := findUsableTokenForService(c.ServerAddress); found {
		klog.V(4).Infof("found token for service: %s", c.ServerAddress)
		authProtocol = yarnauth.AUTH_PROTOCOL_SASL
	}

	err = writeConnectionHeader(con, authProtocol)
	if err != nil {
		return nil, err
	}

	if authProtocol == yarnauth.AUTH_PROTOCOL_SASL {
		klog.V(4).Infof("attempting SASL negotiation.")

		if err = negotiateSimpleTokenAuth(c, con); err != nil {
			klog.Warningf("failed to complete SASL negotiation!")
			return nil, err
		}

	} else {
		klog.V(5).Infof("no usable tokens. proceeding without auth.")
	}

	err = writeConnectionContext(c, con, connectionId, authProtocol)
	if err != nil {
		return nil, err
	}
	//}

	return con, nil
}

func setupConnection(c *Client) (*connection, error) {
	d := net.Dialer{Timeout: connDefaultTimeout}
	conn, err := d.Dial("tcp", c.ServerAddress)
	if err != nil {
		klog.V(4).Infof("error: %v", err)
		return nil, err
	} else {
		klog.V(5).Infof("setup connection success %v", c)
	}

	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, fmt.Errorf("net.TCPConn type assert failed")
	}

	// TODO: Ping thread

	// Set tcp no-delay
	err = tcpConn.SetNoDelay(c.TCPNoDelay)
	if err != nil {
		return nil, err
	}

	return &connection{tcpConn}, nil
}

func writeConnectionHeader(conn *connection, authProtocol yarnauth.AuthProtocol) error {
	if err := conn.con.SetDeadline(time.Now().Add(rwDefaultTimeout)); err != nil {
		return err
	}
	// RPC_HEADER
	if _, err := conn.con.Write(yarnauth.RPC_HEADER); err != nil {
		klog.Warningf("conn.Write yarnauth.RPC_HEADER %v", err)
		return err
	}

	// RPC_VERSION
	if _, err := conn.con.Write(yarnauth.VERSION); err != nil {
		klog.Warningf("conn.Write yarnauth.VERSION %v", err)
		return err
	}

	// RPC_SERVICE_CLASS
	if serviceClass, err := yarnauth.ConvertFixedToBytes(yarnauth.RPC_SERVICE_CLASS); err != nil {
		klog.Warningf("binary.Write %v", err)
		return err
	} else if _, err := conn.con.Write(serviceClass); err != nil {
		klog.Warningf("conn.Write RPC_SERVICE_CLASS %v", err)
		return err
	}

	// AuthProtocol
	if authProtocolBytes, err := yarnauth.ConvertFixedToBytes(authProtocol); err != nil {
		klog.Warningf("WTF AUTH_PROTOCOL %v", err)
		return err
	} else if _, err := conn.con.Write(authProtocolBytes); err != nil {
		klog.Warningf("conn.Write yarnauth.AUTH_PROTOCOL %v", err)
		return err
	}

	return nil
}

func writeConnectionContext(c *Client, conn *connection, connectionId *connection_id, authProtocol yarnauth.AuthProtocol) error {
	if err := conn.con.SetDeadline(time.Now().Add(rwDefaultTimeout)); err != nil {
		return err
	}
	// Create hadoop_common.IpcConnectionContextProto
	ugi, _ := yarnauth.CreateSimpleUGIProto()
	ipcCtxProto := hadoop_common.IpcConnectionContextProto{UserInfo: ugi, Protocol: &connectionId.protocol}

	// Create RpcRequestHeaderProto
	var callId int32 = -3
	var clientId [16]byte = [16]byte(*c.ClientId)

	/*if (authProtocol == yarnauth.AUTH_PROTOCOL_SASL) {
	  callId = SASL_RPC_CALL_ID
	}*/

	rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &yarnauth.RPC_PROTOCOL_BUFFFER, RpcOp: &yarnauth.RPC_FINAL_PACKET, CallId: &callId, ClientId: clientId[0:16], RetryCount: &yarnauth.RPC_DEFAULT_RETRY_COUNT}

	rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
	if err != nil {
		klog.Warningf("proto.Marshal(&rpcReqHeaderProto) %v", err)
		return err
	}

	ipcCtxProtoBytes, err := proto.Marshal(&ipcCtxProto)
	if err != nil {
		klog.Warningf("proto.Marshal(&ipcCtxProto) %v", err)
		return err
	}

	totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(ipcCtxProtoBytes) + sizeVarint(len(ipcCtxProtoBytes))
	var tLen int32 = int32(totalLength)
	totalLengthBytes, err := yarnauth.ConvertFixedToBytes(tLen)

	if err != nil {
		klog.Warningf("ConvertFixedToBytes(totalLength) %v", err)
		return err
	} else if _, err := conn.con.Write(totalLengthBytes); err != nil {
		klog.Warningf("conn.con.Write(totalLengthBytes) %v", err)
		return err
	}

	if err := writeDelimitedBytes(conn, rpcReqHeaderProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, rpcReqHeaderProtoBytes) %v", err)
		return err
	}
	if err := writeDelimitedBytes(conn, ipcCtxProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, ipcCtxProtoBytes) %v", err)
		return err
	}

	return nil
}

func sizeVarint(x int) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func sendRequest(c *Client, conn *connection, rpcCall *call) error {
	klog.V(5).Infof("About to call RPC: %v", rpcCall.procedure)

	if err := conn.con.SetDeadline(time.Now().Add(rwDefaultTimeout)); err != nil {
		return err
	}

	// 0. RpcRequestHeaderProto
	var clientId [16]byte = [16]byte(*c.ClientId)
	rpcReqHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &yarnauth.RPC_PROTOCOL_BUFFFER, RpcOp: &yarnauth.RPC_FINAL_PACKET, CallId: &rpcCall.callId, ClientId: clientId[0:16], RetryCount: &rpcCall.retryCount}
	rpcReqHeaderProtoBytes, err := proto.Marshal(&rpcReqHeaderProto)
	if err != nil {
		klog.Warningf("proto.Marshal(&rpcReqHeaderProto) %v", err)
		return err
	}

	// 1. RequestHeaderProto
	requestHeaderProto := rpcCall.procedure
	requestHeaderProtoBytes, err := proto.Marshal(requestHeaderProto)
	if err != nil {
		klog.Warningf("proto.Marshal(&requestHeaderProto) %v", err)
		return err
	}

	// 2. Param
	paramProto := rpcCall.request
	paramProtoBytes, err := proto.Marshal(paramProto)
	if err != nil {
		klog.Warningf("proto.Marshal(&paramProto) %v", err)
		return err
	}

	totalLength := len(rpcReqHeaderProtoBytes) + sizeVarint(len(rpcReqHeaderProtoBytes)) + len(requestHeaderProtoBytes) + sizeVarint(len(requestHeaderProtoBytes)) + len(paramProtoBytes) + sizeVarint(len(paramProtoBytes))
	var tLen int32 = int32(totalLength)
	if totalLengthBytes, err := yarnauth.ConvertFixedToBytes(tLen); err != nil {
		klog.Warningf("ConvertFixedToBytes(totalLength) %v", err)
		return err
	} else {
		if _, err := conn.con.Write(totalLengthBytes); err != nil {
			klog.Warningf("conn.con.Write(totalLengthBytes) %v", err)
			return err
		}
	}

	if err := writeDelimitedBytes(conn, rpcReqHeaderProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, rpcReqHeaderProtoBytes) %v", err)
		return err
	}
	if err := writeDelimitedBytes(conn, requestHeaderProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, requestHeaderProtoBytes) %v", err)
		return err
	}
	if err := writeDelimitedBytes(conn, paramProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, paramProtoBytes) %v", err)
		return err
	}

	klog.V(5).Infof("Succesfully sent request of length: ", totalLength)

	return nil
}

//func writeDelimitedTo(conn *connection, msg proto.Message) error {
//	msgBytes, err := proto.Marshal(msg)
//	if err != nil {
//		klog.Warningf("proto.Marshal(msg)", err)
//		return err
//	}
//	return writeDelimitedBytes(conn, msgBytes)
//}

func writeDelimitedBytes(conn *connection, data []byte) error {
	if _, err := conn.con.Write(protowire.AppendVarint(nil, uint64(len(data)))); err != nil {
		klog.Warningf("conn.con.Write(proto.EncodeVarint(uint64(len(data)))) %v", err)
		return err
	}
	if _, err := conn.con.Write(data); err != nil {
		klog.Warningf("conn.con.Write(data) %v", err)
		return err
	}

	return nil
}

func (c *Client) readResponse(conn *connection, rpcCall *call) error {
	// Read first 4 bytes to get total-length
	var totalLength int32 = -1
	var totalLengthBytes [4]byte
	if _, err := conn.con.Read(totalLengthBytes[0:4]); err != nil {
		klog.Warningf("conn.con.Read(totalLengthBytes) %v", err)
		return err
	}

	if err := yarnauth.ConvertBytesToFixed(totalLengthBytes[0:4], &totalLength); err != nil {
		klog.Warningf("yarnauth.ConvertBytesToFixed(totalLengthBytes, &totalLength) %v", err)
		return err
	}

	var responseBytes = make([]byte, totalLength)
	reader := bufio.NewReader(conn.con)
	read, err := io.ReadFull(reader, responseBytes)
	if err != nil {
		klog.Warningf("io.ReadFull(reader, responseBytes), %v", err)
		return err
	}
	if int32(read) != totalLength {
		return fmt.Errorf("actural read length %v does not match the total length %v", read, totalLength)
	}

	// Parse RpcResponseHeaderProto
	rpcResponseHeaderProto := hadoop_common.RpcResponseHeaderProto{}
	off, err := readDelimited(responseBytes[0:totalLength], &rpcResponseHeaderProto)
	if err != nil {
		klog.Warningf("readDelimited(responseBytes, rpcResponseHeaderProto) %v", err)
		return err
	}
	klog.V(5).Infof("Received rpcResponseHeaderProto = %v", rpcResponseHeaderProto.String())

	err = c.checkRpcHeader(&rpcResponseHeaderProto)
	if err != nil {
		klog.Warningf("c.checkRpcHeader failed %v", err)
		return err
	}

	if *rpcResponseHeaderProto.Status == hadoop_common.RpcResponseHeaderProto_SUCCESS {
		// Parse RpcResponseWrapper
		_, err = readDelimited(responseBytes[off:], rpcCall.response)
	} else {
		klog.V(4).Infof("RPC failed with status: %v", rpcResponseHeaderProto.Status.String())
		errorDetails := [4]string{rpcResponseHeaderProto.Status.String(), "ServerDidNotSetExceptionClassName", "ServerDidNotSetErrorMsg", "ServerDidNotSetErrorDetail"}
		if rpcResponseHeaderProto.ExceptionClassName != nil {
			errorDetails[0] = *rpcResponseHeaderProto.ExceptionClassName
		}
		if rpcResponseHeaderProto.ErrorMsg != nil {
			errorDetails[1] = *rpcResponseHeaderProto.ErrorMsg
		}
		if rpcResponseHeaderProto.ErrorDetail != nil {
			errorDetails[2] = rpcResponseHeaderProto.ErrorDetail.String()
		}
		err = errors.New(strings.Join(errorDetails[:], ":"))
	}
	return err
}

func readDelimited(rawData []byte, msg proto.Message) (int, error) {
	headerLength, off := protowire.ConsumeVarint(rawData)
	if off == 0 {
		klog.Warningf("proto.DecodeVarint(rawData) returned zero")
		return -1, nil
	}
	b := rawData[off : off+int(headerLength)]
	err := proto.Unmarshal(b, msg)
	if err != nil {
		klog.Warningf("proto.Unmarshal(rawData[off:off+headerLength]) %v", err)
		return -1, err
	}

	return off + int(headerLength), nil
}

func (c *Client) checkRpcHeader(rpcResponseHeaderProto *hadoop_common.RpcResponseHeaderProto) error {
	var callClientId = [16]byte(*c.ClientId)
	var headerClientId = rpcResponseHeaderProto.ClientId
	if rpcResponseHeaderProto.ClientId != nil {
		if !bytes.Equal(callClientId[0:16], headerClientId[0:16]) {
			klog.Warningf("Incorrect clientId: %v", headerClientId)
			return errors.New("Incorrect clientId")
		}
	}
	return nil
}

func sendSaslMessage(c *Client, conn *connection, message *hadoop_common.RpcSaslProto) error {
	saslRpcHeaderProto := hadoop_common.RpcRequestHeaderProto{RpcKind: &yarnauth.RPC_PROTOCOL_BUFFFER,
		RpcOp:      &yarnauth.RPC_FINAL_PACKET,
		CallId:     &SASL_RPC_CALL_ID,
		ClientId:   SASL_RPC_DUMMY_CLIENT_ID,
		RetryCount: &SASL_RPC_INVALID_RETRY_COUNT}

	saslRpcHeaderProtoBytes, err := proto.Marshal(&saslRpcHeaderProto)

	if err != nil {
		klog.Warningf("proto.Marshal(&saslRpcHeaderProto) %v", err)
		return err
	}

	saslRpcMessageProtoBytes, err := proto.Marshal(message)

	if err != nil {
		klog.Warningf("proto.Marshal(saslMessage) %v", err)
		return err
	}

	totalLength := len(saslRpcHeaderProtoBytes) + sizeVarint(len(saslRpcHeaderProtoBytes)) + len(saslRpcMessageProtoBytes) + sizeVarint(len(saslRpcMessageProtoBytes))
	var tLen int32 = int32(totalLength)
	if err := conn.con.SetDeadline(time.Now().Add(rwDefaultTimeout)); err != nil {
		return err
	}
	if totalLengthBytes, err := yarnauth.ConvertFixedToBytes(tLen); err != nil {
		klog.Warningf("ConvertFixedToBytes(totalLength) %v", err)
		return err
	} else {
		if _, err := conn.con.Write(totalLengthBytes); err != nil {
			klog.Warningf("conn.con.Write(totalLengthBytes) %v", err)
			return err
		}
	}
	if err := writeDelimitedBytes(conn, saslRpcHeaderProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, saslRpcHeaderProtoBytes) %v", err)
		return err
	}
	if err := writeDelimitedBytes(conn, saslRpcMessageProtoBytes); err != nil {
		klog.Warningf("writeDelimitedBytes(conn, saslRpcMessageProtoBytes) %v", err)
		return err
	}

	return nil
}

func receiveSaslMessage(c *Client, conn *connection) (*hadoop_common.RpcSaslProto, error) {
	// Read first 4 bytes to get total-length
	var totalLength int32 = -1
	var totalLengthBytes [4]byte

	if err := conn.con.SetDeadline(time.Now().Add(rwDefaultTimeout)); err != nil {
		return nil, err
	}

	if _, err := conn.con.Read(totalLengthBytes[0:4]); err != nil {
		klog.Warningf("conn.con.Read(totalLengthBytes) %v", err)
		return nil, err
	}
	if err := yarnauth.ConvertBytesToFixed(totalLengthBytes[0:4], &totalLength); err != nil {
		klog.Warningf("yarnauth.ConvertBytesToFixed(totalLengthBytes, &totalLength) %v", err)
		return nil, err
	}

	var responseBytes []byte = make([]byte, totalLength)

	if _, err := conn.con.Read(responseBytes); err != nil {
		klog.Warningf("conn.con.Read(totalLengthBytes) %v", err)
		return nil, err
	}

	// Parse RpcResponseHeaderProto
	rpcResponseHeaderProto := hadoop_common.RpcResponseHeaderProto{}
	off, err := readDelimited(responseBytes[0:totalLength], &rpcResponseHeaderProto)
	if err != nil {
		klog.Warningf("readDelimited(responseBytes, rpcResponseHeaderProto) %v", err)
		return nil, err
	}

	err = checkSaslRpcHeader(&rpcResponseHeaderProto)
	if err != nil {
		klog.Warningf("checkSaslRpcHeader failed %v", err)
		return nil, err
	}

	var saslRpcMessage hadoop_common.RpcSaslProto

	if *rpcResponseHeaderProto.Status == hadoop_common.RpcResponseHeaderProto_SUCCESS {
		// Parse RpcResponseWrapper
		if _, err = readDelimited(responseBytes[off:], &saslRpcMessage); err != nil {
			klog.Warningf("failed to read sasl response!")
			return nil, err
		} else {
			return &saslRpcMessage, nil
		}
	} else {
		klog.V(4).Infof("RPC failed with status: %v", rpcResponseHeaderProto.Status.String())
		errorDetails := [4]string{rpcResponseHeaderProto.Status.String(), "ServerDidNotSetExceptionClassName", "ServerDidNotSetErrorMsg", "ServerDidNotSetErrorDetail"}
		if rpcResponseHeaderProto.ExceptionClassName != nil {
			errorDetails[0] = *rpcResponseHeaderProto.ExceptionClassName
		}
		if rpcResponseHeaderProto.ErrorMsg != nil {
			errorDetails[1] = *rpcResponseHeaderProto.ErrorMsg
		}
		if rpcResponseHeaderProto.ErrorDetail != nil {
			errorDetails[2] = rpcResponseHeaderProto.ErrorDetail.String()
		}
		err = errors.New(strings.Join(errorDetails[:], ":"))
		return nil, err
	}
}

func checkSaslRpcHeader(rpcResponseHeaderProto *hadoop_common.RpcResponseHeaderProto) error {
	var headerClientId []byte = rpcResponseHeaderProto.ClientId
	if rpcResponseHeaderProto.ClientId != nil {
		if !bytes.Equal(SASL_RPC_DUMMY_CLIENT_ID, headerClientId) {
			klog.Warningf("Incorrect clientId: %v", headerClientId)
			return errors.New("Incorrect clientId")
		}
	}
	return nil
}

func negotiateSimpleTokenAuth(client *Client, con *connection) error {
	var saslNegotiateState hadoop_common.RpcSaslProto_SaslState = hadoop_common.RpcSaslProto_NEGOTIATE
	var saslNegotiateMessage hadoop_common.RpcSaslProto = hadoop_common.RpcSaslProto{State: &saslNegotiateState}
	var saslResponseMessage *hadoop_common.RpcSaslProto
	var err error

	//send a SASL negotiation request
	if err = sendSaslMessage(client, con, &saslNegotiateMessage); err != nil {
		klog.Warningf("failed to send SASL NEGOTIATE message!")
		return err
	}

	//get a response with supported mehcanisms/challenge
	if saslResponseMessage, err = receiveSaslMessage(client, con); err != nil {
		klog.Warningf("failed to receive SASL NEGOTIATE response!")
		return err
	}

	var auths []*hadoop_common.RpcSaslProto_SaslAuth = saslResponseMessage.GetAuths()

	if numAuths := len(auths); numAuths <= 0 {
		klog.Warningf("No supported auth mechanisms!")
		return errors.New("No supported auth mechanisms!")
	}

	//for now we only support auth when TOKEN/DIGEST-MD5 is the first/only
	//supported auth mechanism
	var auth *hadoop_common.RpcSaslProto_SaslAuth = auths[0]

	if !(auth.GetMethod() == "TOKEN" && auth.GetMechanism() == "DIGEST-MD5") {
		klog.Warningf("yarnauth only supports TOKEN/DIGEST-MD5 auth!")
		return errors.New("yarnauth only supports TOKEN/DIGEST-MD5 auth!")
	}

	method := auth.GetMethod()
	mechanism := auth.GetMechanism()
	protocol := auth.GetProtocol()
	serverId := auth.GetServerId()
	challenge := auth.GetChallenge()

	//TODO: token/service mapping + token selection based on type/service
	//we wouldn't have gotten this far if there wasn't at least one available token.
	userToken, _ := findUsableTokenForService(client.ServerAddress)
	response, err := security.GetDigestMD5ChallengeResponse(protocol, serverId, challenge, userToken)

	if err != nil {
		klog.Warningf("failed to get challenge response! %v", err)
		return err
	}

	saslInitiateState := hadoop_common.RpcSaslProto_INITIATE
	authSend := hadoop_common.RpcSaslProto_SaslAuth{Method: &method, Mechanism: &mechanism,
		Protocol: &protocol, ServerId: &serverId}
	authsSendArray := []*hadoop_common.RpcSaslProto_SaslAuth{&authSend}
	saslInitiateMessage := hadoop_common.RpcSaslProto{State: &saslInitiateState,
		Token: []byte(response), Auths: authsSendArray}

	//send a SASL inititate request
	if err = sendSaslMessage(client, con, &saslInitiateMessage); err != nil {
		klog.Warningf("failed to send SASL INITIATE message!")
		return err
	}

	//get a response with supported mehcanisms/challenge
	if saslResponseMessage, err = receiveSaslMessage(client, con); err != nil {
		klog.Warningf("failed to read response to SASL INITIATE response!")
		return err
	}

	if saslResponseMessage.GetState() != hadoop_common.RpcSaslProto_SUCCESS {
		klog.Warningf("expected SASL SUCCESS response!")
		return errors.New("expected SASL SUCCESS response!")
	}

	klog.V(4).Infof("Successfully completed SASL negotiation!")

	return nil //errors.New("abort here")
}
