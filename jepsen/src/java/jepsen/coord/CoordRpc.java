package jepsen.coord;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.OneofDescriptorProto;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.MetadataUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

/**
 * Wire-compatible gRPC layer for the coord API, built without protoc-generated
 * stubs. Constructs the exact FileDescriptors from the coord contract
 * (field numbers / service names match coord-proto 100%), then exposes
 * MethodDescriptors over DynamicMessage with a custom marshaller.
 */
public class CoordRpc {

  private static final Type T_STRING = Type.TYPE_STRING;
  private static final Type T_BYTES  = Type.TYPE_BYTES;
  private static final Type T_INT64  = Type.TYPE_INT64;
  private static final Type T_BOOL   = Type.TYPE_BOOL;
  private static final Type T_MSG    = Type.TYPE_MESSAGE;
  private static final Type T_ENUM   = Type.TYPE_ENUM;

  private static final Label L_OPT = Label.LABEL_OPTIONAL;
  private static final Label L_REP = Label.LABEL_REPEATED;

  // ------------------------------------------------------------------
  // Field builders
  // ------------------------------------------------------------------

  private static FieldDescriptorProto.Builder f(String name, int num, Type t, Label l) {
    return FieldDescriptorProto.newBuilder().setName(name).setNumber(num).setType(t).setLabel(l);
  }

  private static FieldDescriptorProto.Builder str(String name, int num) {
    return f(name, num, T_STRING, L_OPT);
  }

  private static FieldDescriptorProto.Builder bytes(String name, int num) {
    return f(name, num, T_BYTES, L_OPT);
  }

  private static FieldDescriptorProto.Builder i64(String name, int num) {
    return f(name, num, T_INT64, L_OPT);
  }

  private static FieldDescriptorProto.Builder bool(String name, int num) {
    return f(name, num, T_BOOL, L_OPT);
  }

  private static FieldDescriptorProto.Builder msg(String name, int num, String typeName) {
    return f(name, num, T_MSG, L_OPT).setTypeName(typeName);
  }

  private static FieldDescriptorProto.Builder repMsg(String name, int num, String typeName) {
    return f(name, num, T_MSG, L_REP).setTypeName(typeName);
  }

  private static FieldDescriptorProto.Builder enumF(String name, int num, String typeName) {
    return f(name, num, T_ENUM, L_OPT).setTypeName(typeName);
  }

  private static FieldDescriptorProto.Builder repStr(String name, int num) {
    return f(name, num, T_STRING, L_REP);
  }

  private static FieldDescriptorProto.Builder oneofMsg(String name, int num, String typeName,
                                                       int oneofIndex) {
    return f(name, num, T_MSG, L_OPT).setTypeName(typeName).setOneofIndex(oneofIndex);
  }

  private static OneofDescriptorProto oneof(String name) {
    return OneofDescriptorProto.newBuilder().setName(name).build();
  }

  // ------------------------------------------------------------------
  // coord/kv/kv.proto
  // ------------------------------------------------------------------

  private static DescriptorProto kvKeyValue() {
    return DescriptorProto.newBuilder().setName("KeyValue")
        .addField(bytes("key", 1).build())
        .addField(bytes("value", 2).build())
        .addField(i64("create_revision", 3).build())
        .addField(i64("mod_revision", 4).build())
        .addField(i64("version", 5).build())
        .addField(i64("lease_id", 6).build())
        .build();
  }

  private static DescriptorProto kvPutRequest() {
    return DescriptorProto.newBuilder().setName("PutRequest")
        .addField(bytes("key", 1).build())
        .addField(bytes("value", 2).build())
        .addField(i64("lease_id", 3).build())
        .addField(bool("prev_kv", 4).build())
        .addField(bytes("request_id", 5).build())
        .build();
  }

  private static DescriptorProto kvPutResponse() {
    return DescriptorProto.newBuilder().setName("PutResponse")
        .addField(msg("prev_kv", 1, ".coord.kv.KeyValue").build())
        .addField(i64("revision", 2).build())
        .build();
  }

  private static DescriptorProto kvRangeRequest() {
    return DescriptorProto.newBuilder().setName("RangeRequest")
        .addField(bytes("key", 1).build())
        .addField(bytes("range_end", 2).build())
        .addField(i64("limit", 3).build())
        .addField(i64("revision", 4).build())
        .addField(bool("keys_only", 5).build())
        .addField(bool("count_only", 6).build())
        .build();
  }

  private static DescriptorProto kvRangeResponse() {
    return DescriptorProto.newBuilder().setName("RangeResponse")
        .addField(repMsg("kvs", 1, ".coord.kv.KeyValue").build())
        .addField(i64("count", 2).build())
        .addField(i64("revision", 3).build())
        .build();
  }

  private static DescriptorProto kvDeleteRequest() {
    return DescriptorProto.newBuilder().setName("DeleteRequest")
        .addField(bytes("key", 1).build())
        .addField(bytes("range_end", 2).build())
        .addField(bool("prev_kv", 3).build())
        .addField(bytes("request_id", 4).build())
        .build();
  }

  private static DescriptorProto kvDeleteResponse() {
    return DescriptorProto.newBuilder().setName("DeleteResponse")
        .addField(i64("deleted", 1).build())
        .addField(repMsg("prev_kvs", 2, ".coord.kv.KeyValue").build())
        .addField(i64("revision", 3).build())
        .build();
  }

  private static ServiceDescriptorProto kvService() {
    return ServiceDescriptorProto.newBuilder().setName("KV")
        .addMethod(MethodDescriptorProto.newBuilder().setName("Put")
            .setInputType(".coord.kv.PutRequest").setOutputType(".coord.kv.PutResponse"))
        .addMethod(MethodDescriptorProto.newBuilder().setName("Range")
            .setInputType(".coord.kv.RangeRequest").setOutputType(".coord.kv.RangeResponse"))
        .addMethod(MethodDescriptorProto.newBuilder().setName("Delete")
            .setInputType(".coord.kv.DeleteRequest").setOutputType(".coord.kv.DeleteResponse"))
        .build();
  }

  private static FileDescriptorProto kvFileProto() {
    return FileDescriptorProto.newBuilder()
        .setName("coord/kv/kv.proto")
        .setPackage("coord.kv")
        .setSyntax("proto3")
        .addMessageType(kvKeyValue())
        .addMessageType(kvPutRequest())
        .addMessageType(kvPutResponse())
        .addMessageType(kvRangeRequest())
        .addMessageType(kvRangeResponse())
        .addMessageType(kvDeleteRequest())
        .addMessageType(kvDeleteResponse())
        .addService(kvService())
        .build();
  }

  // ------------------------------------------------------------------
  // coord/txn/txn.proto (imports coord/kv/kv.proto)
  // ------------------------------------------------------------------

  private static EnumDescriptorProto compareResult() {
    return EnumDescriptorProto.newBuilder().setName("CompareResult")
        .addValue(EnumValueDescriptorProto.newBuilder().setName("EQUAL").setNumber(0).build())
        .addValue(EnumValueDescriptorProto.newBuilder().setName("GREATER").setNumber(1).build())
        .addValue(EnumValueDescriptorProto.newBuilder().setName("LESS").setNumber(2).build())
        .addValue(EnumValueDescriptorProto.newBuilder().setName("NOT_EQUAL").setNumber(3).build())
        .build();
  }

  private static EnumDescriptorProto target() {
    return EnumDescriptorProto.newBuilder().setName("Target")
        .addValue(EnumValueDescriptorProto.newBuilder().setName("VERSION").setNumber(0).build())
        .addValue(EnumValueDescriptorProto.newBuilder().setName("VALUE").setNumber(1).build())
        .addValue(EnumValueDescriptorProto.newBuilder().setName("MOD_REV").setNumber(2).build())
        .build();
  }

  private static DescriptorProto txnCompare() {
    return DescriptorProto.newBuilder().setName("Compare")
        .addEnumType(compareResult())
        .addEnumType(target())
        .addField(enumF("result", 1, ".coord.txn.Compare.CompareResult").build())
        .addField(enumF("target", 2, ".coord.txn.Compare.Target").build())
        .addField(bytes("key", 3).build())
        .addOneofDecl(oneof("target_value"))
        .addField(i64("version", 4).setOneofIndex(0).build())
        .addField(bytes("value", 5).setOneofIndex(0).build())
        .addField(i64("mod_revision", 6).setOneofIndex(0).build())
        .build();
  }

  private static DescriptorProto txnRequestOp() {
    return DescriptorProto.newBuilder().setName("RequestOp")
        .addOneofDecl(oneof("op"))
        .addField(oneofMsg("request_range", 1, ".coord.kv.RangeRequest", 0).build())
        .addField(oneofMsg("request_put", 2, ".coord.kv.PutRequest", 0).build())
        .addField(oneofMsg("request_delete", 3, ".coord.kv.DeleteRequest", 0).build())
        .build();
  }

  private static DescriptorProto txnResponseOp() {
    return DescriptorProto.newBuilder().setName("ResponseOp")
        .addOneofDecl(oneof("op"))
        .addField(oneofMsg("response_range", 1, ".coord.kv.RangeResponse", 0).build())
        .addField(oneofMsg("response_put", 2, ".coord.kv.PutResponse", 0).build())
        .addField(oneofMsg("response_delete", 3, ".coord.kv.DeleteResponse", 0).build())
        .build();
  }

  private static DescriptorProto txnRequest() {
    return DescriptorProto.newBuilder().setName("TxnRequest")
        .addField(repMsg("compare", 1, ".coord.txn.Compare").build())
        .addField(repMsg("success", 2, ".coord.txn.RequestOp").build())
        .addField(repMsg("failure", 3, ".coord.txn.RequestOp").build())
        .addField(bytes("request_id", 4).build())
        .build();
  }

  private static DescriptorProto txnResponse() {
    return DescriptorProto.newBuilder().setName("TxnResponse")
        .addField(bool("succeeded", 1).build())
        .addField(repMsg("responses", 2, ".coord.txn.ResponseOp").build())
        .addField(i64("revision", 3).build())
        .build();
  }

  private static ServiceDescriptorProto txnService() {
    return ServiceDescriptorProto.newBuilder().setName("Txn")
        .addMethod(MethodDescriptorProto.newBuilder().setName("Txn")
            .setInputType(".coord.txn.TxnRequest").setOutputType(".coord.txn.TxnResponse"))
        .build();
  }

  private static FileDescriptorProto txnFileProto() {
    return FileDescriptorProto.newBuilder()
        .setName("coord/txn/txn.proto")
        .setPackage("coord.txn")
        .setSyntax("proto3")
        .addDependency("coord/kv/kv.proto")
        .addMessageType(txnCompare())
        .addMessageType(txnRequestOp())
        .addMessageType(txnResponseOp())
        .addMessageType(txnRequest())
        .addMessageType(txnResponse())
        .addService(txnService())
        .build();
  }

  // ------------------------------------------------------------------
  // coord/maintenance/maintenance.proto
  // ------------------------------------------------------------------

  private static FileDescriptorProto maintenanceFileProto() {
    return FileDescriptorProto.newBuilder()
        .setName("coord/maintenance/maintenance.proto")
        .setPackage("coord.maintenance")
        .setSyntax("proto3")
        .addMessageType(DescriptorProto.newBuilder().setName("StatusRequest").build())
        .addMessageType(DescriptorProto.newBuilder().setName("StatusResponse")
            .addField(i64("revision", 1).build()).build())
        .addService(ServiceDescriptorProto.newBuilder().setName("Maintenance")
            .addMethod(MethodDescriptorProto.newBuilder().setName("Status")
                .setInputType(".coord.maintenance.StatusRequest")
                .setOutputType(".coord.maintenance.StatusResponse")).build())
        .build();
  }

  // ------------------------------------------------------------------
  // coord/auth/auth.proto
  // ------------------------------------------------------------------

  private static DescriptorProto authAuthenticateRequest() {
    return DescriptorProto.newBuilder().setName("AuthenticateRequest")
        .addField(str("name", 1).build())
        .addField(str("password", 2).build())
        .build();
  }

  private static DescriptorProto authAuthenticateResponse() {
    return DescriptorProto.newBuilder().setName("AuthenticateResponse")
        .addField(str("token", 1).build())
        .addField(str("cct", 2).build())
        .addField(i64("expires_at", 3).build())
        .addField(repStr("roles", 4).build())
        .addField(str("refresh_token", 5).build())
        .addField(i64("refresh_expires_at", 6).build())
        .build();
  }

  private static DescriptorProto authRefreshTokenRequest() {
    return DescriptorProto.newBuilder().setName("RefreshTokenRequest")
        .addField(str("refresh_token", 1).build())
        .build();
  }

  private static ServiceDescriptorProto authService() {
    return ServiceDescriptorProto.newBuilder().setName("Auth")
        .addMethod(MethodDescriptorProto.newBuilder().setName("Authenticate")
            .setInputType(".coord.auth.AuthenticateRequest")
            .setOutputType(".coord.auth.AuthenticateResponse"))
        .addMethod(MethodDescriptorProto.newBuilder().setName("RefreshToken")
            .setInputType(".coord.auth.RefreshTokenRequest")
            .setOutputType(".coord.auth.AuthenticateResponse"))
        .build();
  }

  private static FileDescriptorProto authFileProto() {
    return FileDescriptorProto.newBuilder()
        .setName("coord/auth/auth.proto")
        .setPackage("coord.auth")
        .setSyntax("proto3")
        .addMessageType(authAuthenticateRequest())
        .addMessageType(authAuthenticateResponse())
        .addMessageType(authRefreshTokenRequest())
        .addService(authService())
        .build();
  }

  // ------------------------------------------------------------------
  // coord/watch/watch.proto
  // ------------------------------------------------------------------

  private static DescriptorProto watchCreateRequest() {
    return DescriptorProto.newBuilder().setName("WatchCreateRequest")
        .addField(bytes("key", 1).build())
        .addField(bytes("range_end", 2).build())
        .addField(i64("start_revision", 3).build())
        .addField(bool("prev_kv", 4).build())
        .build();
  }

  private static DescriptorProto watchEvent() {
    return DescriptorProto.newBuilder().setName("WatchEvent")
        .addField(enumF("type", 1, ".coord.watch.WatchEvent.EventType").build())
        .addField(repMsg("kvs", 2, ".coord.kv.KeyValue").build())
        .addField(msg("prev_kv", 3, ".coord.kv.KeyValue").build())
        .addField(i64("revision", 4).build())
        .addEnumType(EnumDescriptorProto.newBuilder().setName("EventType")
            .addValue(EnumValueDescriptorProto.newBuilder().setName("PUT").setNumber(0))
            .addValue(EnumValueDescriptorProto.newBuilder().setName("DELETE").setNumber(1))
            .addValue(EnumValueDescriptorProto.newBuilder()
                .setName("BUFFER_OVERFLOW").setNumber(2))
            .addValue(EnumValueDescriptorProto.newBuilder()
                .setName("HISTORY_UNAVAILABLE").setNumber(3)))
        .build();
  }

  private static DescriptorProto watchRequest() {
    return DescriptorProto.newBuilder().setName("WatchRequest")
        .addField(oneofMsg("create", 1, ".coord.watch.WatchCreateRequest", 0))
        .addOneofDecl(oneof("request"))
        .build();
  }

  private static DescriptorProto watchResponse() {
    return DescriptorProto.newBuilder().setName("WatchResponse")
        .addField(i64("watch_id", 1).build())
        .addField(repMsg("events", 2, ".coord.watch.WatchEvent").build())
        .build();
  }

  private static FileDescriptorProto watchFileProto() {
    return FileDescriptorProto.newBuilder()
        .setName("coord/watch/watch.proto")
        .setPackage("coord.watch")
        .setSyntax("proto3")
        .addDependency("coord/kv/kv.proto")
        .addMessageType(watchCreateRequest())
        .addMessageType(watchEvent())
        .addMessageType(watchRequest())
        .addMessageType(watchResponse())
        .addService(ServiceDescriptorProto.newBuilder().setName("Watch")
            .addMethod(MethodDescriptorProto.newBuilder().setName("Watch")
                .setInputType(".coord.watch.WatchRequest")
                .setOutputType(".coord.watch.WatchResponse")
                .setClientStreaming(true)
                .setServerStreaming(true)))
        .build();
  }

  // ------------------------------------------------------------------
  // coord/lease/lease.proto
  // ------------------------------------------------------------------

  private static DescriptorProto leaseGrantRequest() {
    return DescriptorProto.newBuilder().setName("LeaseGrantRequest")
        .addField(i64("ttl", 1).build())
        .addField(i64("id", 2).build())
        .build();
  }

  private static DescriptorProto leaseGrantResponse() {
    return DescriptorProto.newBuilder().setName("LeaseGrantResponse")
        .addField(i64("id", 1).build())
        .addField(i64("ttl", 2).build())
        .addField(str("error", 3).build())
        .build();
  }

  private static DescriptorProto leaseRevokeRequest() {
    return DescriptorProto.newBuilder().setName("LeaseRevokeRequest")
        .addField(i64("id", 1).build())
        .build();
  }

  private static DescriptorProto leaseRevokeResponse() {
    return DescriptorProto.newBuilder().setName("LeaseRevokeResponse").build();
  }

  private static DescriptorProto leaseKeepAliveRequest() {
    return DescriptorProto.newBuilder().setName("LeaseKeepAliveRequest")
        .addField(i64("id", 1).build())
        .build();
  }

  private static DescriptorProto leaseKeepAliveResponse() {
    return DescriptorProto.newBuilder().setName("LeaseKeepAliveResponse")
        .addField(i64("id", 1).build())
        .addField(i64("ttl", 2).build())
        .build();
  }

  private static FileDescriptorProto leaseFileProto() {
    return FileDescriptorProto.newBuilder()
        .setName("coord/lease/lease.proto")
        .setPackage("coord.lease")
        .setSyntax("proto3")
        .addMessageType(leaseGrantRequest())
        .addMessageType(leaseGrantResponse())
        .addMessageType(leaseRevokeRequest())
        .addMessageType(leaseRevokeResponse())
        .addMessageType(leaseKeepAliveRequest())
        .addMessageType(leaseKeepAliveResponse())
        .addService(ServiceDescriptorProto.newBuilder().setName("Lease")
            .addMethod(MethodDescriptorProto.newBuilder().setName("LeaseGrant")
                .setInputType(".coord.lease.LeaseGrantRequest")
                .setOutputType(".coord.lease.LeaseGrantResponse"))
            .addMethod(MethodDescriptorProto.newBuilder().setName("LeaseRevoke")
                .setInputType(".coord.lease.LeaseRevokeRequest")
                .setOutputType(".coord.lease.LeaseRevokeResponse"))
            .addMethod(MethodDescriptorProto.newBuilder().setName("LeaseKeepAlive")
                .setInputType(".coord.lease.LeaseKeepAliveRequest")
                .setOutputType(".coord.lease.LeaseKeepAliveResponse")
                .setClientStreaming(true)
                .setServerStreaming(true)))
        .build();
  }

  // ------------------------------------------------------------------
  // Build FileDescriptors
  // ------------------------------------------------------------------

  private static FileDescriptor build(FileDescriptorProto proto, FileDescriptor[] deps) {
    try {
      return FileDescriptor.buildFrom(proto, deps, true);
    } catch (Descriptors.DescriptorValidationException e) {
      throw new RuntimeException("Failed to build descriptor for " + proto.getName(), e);
    }
  }

  public static final FileDescriptor KV_FILE = build(kvFileProto(), new FileDescriptor[0]);
  public static final FileDescriptor TXN_FILE = build(txnFileProto(), new FileDescriptor[]{KV_FILE});
  public static final FileDescriptor MAINT_FILE = build(maintenanceFileProto(), new FileDescriptor[0]);
  public static final FileDescriptor AUTH_FILE = build(authFileProto(), new FileDescriptor[0]);
  public static final FileDescriptor WATCH_FILE = build(watchFileProto(),
      new FileDescriptor[]{KV_FILE});
  public static final FileDescriptor LEASE_FILE = build(leaseFileProto(), new FileDescriptor[0]);

  public static final Descriptor KV_KEY_VALUE = KV_FILE.findMessageTypeByName("KeyValue");
  public static final Descriptor KV_PUT_REQUEST = KV_FILE.findMessageTypeByName("PutRequest");
  public static final Descriptor KV_PUT_RESPONSE = KV_FILE.findMessageTypeByName("PutResponse");
  public static final Descriptor KV_RANGE_REQUEST = KV_FILE.findMessageTypeByName("RangeRequest");
  public static final Descriptor KV_RANGE_RESPONSE = KV_FILE.findMessageTypeByName("RangeResponse");
  public static final Descriptor KV_DELETE_REQUEST = KV_FILE.findMessageTypeByName("DeleteRequest");
  public static final Descriptor KV_DELETE_RESPONSE = KV_FILE.findMessageTypeByName("DeleteResponse");

  public static final Descriptor TXN_COMPARE = TXN_FILE.findMessageTypeByName("Compare");
  public static final Descriptor TXN_REQUEST_OP = TXN_FILE.findMessageTypeByName("RequestOp");
  public static final Descriptor TXN_RESPONSE_OP = TXN_FILE.findMessageTypeByName("ResponseOp");
  public static final Descriptor TXN_REQUEST = TXN_FILE.findMessageTypeByName("TxnRequest");
  public static final Descriptor TXN_RESPONSE = TXN_FILE.findMessageTypeByName("TxnResponse");

  public static final Descriptor MAINT_STATUS_REQUEST = MAINT_FILE.findMessageTypeByName("StatusRequest");
  public static final Descriptor MAINT_STATUS_RESPONSE = MAINT_FILE.findMessageTypeByName("StatusResponse");

  public static final Descriptor AUTH_AUTHENTICATE_REQUEST = AUTH_FILE.findMessageTypeByName("AuthenticateRequest");
  public static final Descriptor AUTH_AUTHENTICATE_RESPONSE = AUTH_FILE.findMessageTypeByName("AuthenticateResponse");
  public static final Descriptor AUTH_REFRESH_TOKEN_REQUEST = AUTH_FILE.findMessageTypeByName("RefreshTokenRequest");

  public static final Descriptor WATCH_CREATE_REQUEST =
      WATCH_FILE.findMessageTypeByName("WatchCreateRequest");
  public static final Descriptor WATCH_EVENT = WATCH_FILE.findMessageTypeByName("WatchEvent");
  public static final Descriptor WATCH_REQUEST = WATCH_FILE.findMessageTypeByName("WatchRequest");
  public static final Descriptor WATCH_RESPONSE = WATCH_FILE.findMessageTypeByName("WatchResponse");

  public static final Descriptor LEASE_GRANT_REQUEST =
      LEASE_FILE.findMessageTypeByName("LeaseGrantRequest");
  public static final Descriptor LEASE_GRANT_RESPONSE =
      LEASE_FILE.findMessageTypeByName("LeaseGrantResponse");
  public static final Descriptor LEASE_REVOKE_REQUEST =
      LEASE_FILE.findMessageTypeByName("LeaseRevokeRequest");
  public static final Descriptor LEASE_REVOKE_RESPONSE =
      LEASE_FILE.findMessageTypeByName("LeaseRevokeResponse");
  public static final Descriptor LEASE_KEEPALIVE_REQUEST =
      LEASE_FILE.findMessageTypeByName("LeaseKeepAliveRequest");
  public static final Descriptor LEASE_KEEPALIVE_RESPONSE =
      LEASE_FILE.findMessageTypeByName("LeaseKeepAliveResponse");

  // ------------------------------------------------------------------
  // gRPC method descriptors over DynamicMessage
  // ------------------------------------------------------------------

  /** A marshaller that serializes/parses DynamicMessages via their descriptor. */
  public static final class DynMarshaller implements MethodDescriptor.Marshaller<DynamicMessage> {
    private final Descriptor desc;

    public DynMarshaller(Descriptor desc) {
      this.desc = desc;
    }

    @Override
    public InputStream stream(DynamicMessage value) {
      return new ByteArrayInputStream(value.toByteArray());
    }

    @Override
    public DynamicMessage parse(InputStream stream) {
      try {
        return DynamicMessage.parseFrom(desc, stream);
      } catch (IOException e) {
        throw new RuntimeException("Failed to parse " + desc.getFullName(), e);
      }
    }
  }

  private static MethodDescriptor<DynamicMessage, DynamicMessage> unary(
      String fullName, Descriptor req, Descriptor resp) {
    return MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
        .setType(MethodDescriptor.MethodType.UNARY)
        .setFullMethodName(fullName)
        .setRequestMarshaller(new DynMarshaller(req))
        .setResponseMarshaller(new DynMarshaller(resp))
        .build();
  }

  public static final MethodDescriptor<DynamicMessage, DynamicMessage> PUT =
      unary("coord.kv.KV/Put", KV_PUT_REQUEST, KV_PUT_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> RANGE =
      unary("coord.kv.KV/Range", KV_RANGE_REQUEST, KV_RANGE_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> DELETE =
      unary("coord.kv.KV/Delete", KV_DELETE_REQUEST, KV_DELETE_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> TXN =
      unary("coord.txn.Txn/Txn", TXN_REQUEST, TXN_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> STATUS =
      unary("coord.maintenance.Maintenance/Status", MAINT_STATUS_REQUEST, MAINT_STATUS_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> AUTHENTICATE =
      unary("coord.auth.Auth/Authenticate", AUTH_AUTHENTICATE_REQUEST, AUTH_AUTHENTICATE_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> REFRESH_TOKEN =
      unary("coord.auth.Auth/RefreshToken", AUTH_REFRESH_TOKEN_REQUEST, AUTH_AUTHENTICATE_RESPONSE);

  public static final MethodDescriptor<DynamicMessage, DynamicMessage> LEASE_GRANT =
      unary("coord.lease.Lease/LeaseGrant", LEASE_GRANT_REQUEST, LEASE_GRANT_RESPONSE);
  public static final MethodDescriptor<DynamicMessage, DynamicMessage> LEASE_REVOKE =
      unary("coord.lease.Lease/LeaseRevoke", LEASE_REVOKE_REQUEST, LEASE_REVOKE_RESPONSE);

  // ------------------------------------------------------------------
  // Channels / calls
  // ------------------------------------------------------------------

  public static ManagedChannel channel(String host, int port) {
    return ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
  }

  /** Returns a channel wrapper that attaches `authorization: Bearer <cct>`. */
  public static Channel withAuth(Channel channel, String cct) {
    Metadata md = new Metadata();
    md.put(Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER), "Bearer " + cct);
    return ClientInterceptors.intercept(channel, MetadataUtils.newAttachHeadersInterceptor(md));
  }

  /** Blocking unary call with a per-call deadline (ms). Throws StatusRuntimeException. */
  public static DynamicMessage call(Channel channel,
                                    MethodDescriptor<DynamicMessage, DynamicMessage> method,
                                    DynamicMessage request, long timeoutMs) {
    CallOptions opts = CallOptions.DEFAULT.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    ClientCall<DynamicMessage, DynamicMessage> call = channel.newCall(method, opts);
    return ClientCalls.blockingUnaryCall(call, request);
  }

  /** Convenience: new empty DynamicMessage for a descriptor. */
  public static DynamicMessage message(Descriptor desc) {
    return DynamicMessage.newBuilder(desc).build();
  }

  // ------------------------------------------------------------------
  // Watch (bidi streaming)
  // ------------------------------------------------------------------

  public static final MethodDescriptor<DynamicMessage, DynamicMessage> WATCH =
      MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
          .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
          .setFullMethodName("coord.watch.Watch/Watch")
          .setRequestMarshaller(new DynMarshaller(WATCH_REQUEST))
          .setResponseMarshaller(new DynMarshaller(WATCH_RESPONSE))
          .build();

  /** One message received from a watch stream: either a response or a stream
   *  termination (error). */
  public static final class WatchMsg {
    /** The WatchResponse, or null when this is a termination marker. */
    public final DynamicMessage response;
    /** Non-null on termination: the gRPC error message (or "closed"). */
    public final String error;
    /** Client-side receive timestamp (System.nanoTime), for latency accounting. */
    public final long nanoTime;

    WatchMsg(DynamicMessage response, String error, long nanoTime) {
      this.response = response;
      this.error = error;
      this.nanoTime = nanoTime;
    }

    public boolean isError() { return response == null; }
    public String toString() {
      return isError() ? ("WatchMsg(error " + error + ")")
                       : ("WatchMsg(" + response + ")");
    }
  }

  /**
   * A bidirectional Watch stream driven from Clojure.
   *
   * The contract (coord/watch/watch.proto) says a client cancels a watch by
   * closing the stream, so {@link #close()} cancels the call; that is the
   * primitive every resume/preemption test needs.
   *
   * Responses are read on a daemon thread and pushed onto a bounded queue, so a
   * caller can (a) poll with a timeout, (b) let the stream run while doing other
   * work, and (c) observe stream termination as a {@link WatchMsg} marker
   * instead of an exception on some other thread. If the queue is full the
   * reader blocks — that is deliberate backpressure, and it is the situation
   * that makes coord's own server-side buffer overflow (BUFFER_OVERFLOW event)
   * observable.
   */
  public static final class Watcher implements java.io.Closeable {
    private final ClientCall<DynamicMessage, DynamicMessage> call;
    private final java.util.concurrent.BlockingQueue<WatchMsg> queue;
    private volatile boolean closed = false;

    public Watcher(Channel channel, DynamicMessage createRequest, int queueSize) {
      this.call = channel.newCall(WATCH, CallOptions.DEFAULT);
      this.queue = new java.util.concurrent.ArrayBlockingQueue<>(queueSize);
      final Watcher self = this;
      call.start(new ClientCall.Listener<DynamicMessage>() {
        @Override
        public void onMessage(DynamicMessage msg) {
          put(new WatchMsg(msg, null, System.nanoTime()));
          // ask for the next one: without this we would receive exactly one
          // message and the stream would stall.
          call.request(1);
        }

        @Override
        public void onClose(io.grpc.Status status, Metadata trailers) {
          String err = status.isOk() ? "closed"
                                     : (status.getCode() + ": " + status.getDescription());
          put(new WatchMsg(null, err, System.nanoTime()));
        }

        @Override
        public void onHeaders(Metadata headers) { }

        private void put(WatchMsg m) {
          try {
            // Bounded wait: if the consumer vanished, drop rather than hang the
            // reader thread forever.
            if (!queue.offer(m, 30, TimeUnit.SECONDS)) {
              queue.clear();
              queue.offer(m);
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }, new Metadata());

      // half-close semantics are fine here: we send exactly one create request
      // and then keep the stream open for responses until close().
      call.sendMessage(createRequest);
      call.request(1);
    }

    /** Blocking poll; returns null on timeout. */
    public WatchMsg poll(long timeoutMs) throws InterruptedException {
      return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Non-blocking poll. */
    public WatchMsg tryPoll() { return queue.poll(); }

    /** Number of messages buffered (never delivered to the caller yet). */
    public int buffered() { return queue.size(); }

    public boolean isClosed() { return closed; }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        try {
          call.cancel("jepsen: watcher closed", null);
        } catch (RuntimeException ignore) {
          // already cancelled / channel gone
        }
      }
    }
  }

  /** Opens a watch stream. `queueSize` bounds the client-side receive buffer. */
  public static Watcher watch(Channel channel, DynamicMessage createRequest, int queueSize) {
    return new Watcher(channel, createRequest, queueSize);
  }

  // ------------------------------------------------------------------
  // Lease keep-alive (bidi streaming)
  // ------------------------------------------------------------------

  public static final MethodDescriptor<DynamicMessage, DynamicMessage> LEASE_KEEPALIVE =
      MethodDescriptor.<DynamicMessage, DynamicMessage>newBuilder()
          .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
          .setFullMethodName("coord.lease.Lease/LeaseKeepAlive")
          .setRequestMarshaller(new DynMarshaller(LEASE_KEEPALIVE_REQUEST))
          .setResponseMarshaller(new DynMarshaller(LEASE_KEEPALIVE_RESPONSE))
          .build();

  /** One message received from a keep-alive stream: a response or a
   *  termination marker (same shape as {@link WatchMsg}). */
  public static final class KeepAliveMsg {
    public final DynamicMessage response;
    public final String error;
    public final long nanoTime;

    KeepAliveMsg(DynamicMessage response, String error, long nanoTime) {
      this.response = response;
      this.error = error;
      this.nanoTime = nanoTime;
    }

    public boolean isError() { return response == null; }
    public String toString() {
      return isError() ? ("KeepAliveMsg(error " + error + ")")
                       : ("KeepAliveMsg(" + response + ")");
    }
  }

  /**
   * A bidirectional LeaseKeepAlive stream driven from Clojure.
   *
   * Unlike {@link Watcher} (one create request, then receive-only), the
   * contract's keep-alive stream is a genuine request/response dialogue: the
   * client sends LeaseKeepAliveRequest on a cadence and the server answers each
   * one with the remaining TTL (ttl=0 meaning "lease is gone, re-grant").
   *
   * Responses are read on a daemon thread into a bounded queue so the caller
   * can send on its own schedule and poll for answers, while stream
   * termination (kill / partition / server restart) shows up as an
   * {@link KeepAliveMsg} error marker instead of an exception on another
   * thread.
   */
  public static final class KeepAliver implements java.io.Closeable {
    private final ClientCall<DynamicMessage, DynamicMessage> call;
    private final java.util.concurrent.BlockingQueue<KeepAliveMsg> queue;
    private volatile boolean closed = false;

    public KeepAliver(Channel channel, int queueSize) {
      this.call = channel.newCall(LEASE_KEEPALIVE, CallOptions.DEFAULT);
      this.queue = new java.util.concurrent.ArrayBlockingQueue<>(queueSize);
      call.start(new ClientCall.Listener<DynamicMessage>() {
        @Override
        public void onMessage(DynamicMessage msg) {
          put(new KeepAliveMsg(msg, null, System.nanoTime()));
          // ask for the next one, otherwise the stream stalls after one answer
          call.request(1);
        }

        @Override
        public void onClose(io.grpc.Status status, Metadata trailers) {
          String err = status.isOk() ? "closed"
                                     : (status.getCode() + ": " + status.getDescription());
          put(new KeepAliveMsg(null, err, System.nanoTime()));
        }

        @Override
        public void onHeaders(Metadata headers) { }

        private void put(KeepAliveMsg m) {
          try {
            if (!queue.offer(m, 30, TimeUnit.SECONDS)) {
              queue.clear();
              queue.offer(m);
            }
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }, new Metadata());
      call.request(1);
    }

    /** Sends one keep-alive request for `leaseId`. */
    public void send(long leaseId) {
      DynamicMessage req = DynamicMessage.newBuilder(LEASE_KEEPALIVE_REQUEST)
          .setField(LEASE_KEEPALIVE_REQUEST.findFieldByName("id"), leaseId)
          .build();
      call.sendMessage(req);
    }

    /** Blocking poll; returns null on timeout. */
    public KeepAliveMsg poll(long timeoutMs) throws InterruptedException {
      return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** Non-blocking poll. */
    public KeepAliveMsg tryPoll() { return queue.poll(); }

    public int buffered() { return queue.size(); }

    public boolean isClosed() { return closed; }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        try {
          call.cancel("jepsen: keep-aliver closed", null);
        } catch (RuntimeException ignore) {
          // already cancelled / channel gone
        }
      }
    }
  }

  /** Opens a LeaseKeepAlive stream. */
  public static KeepAliver keepAlive(Channel channel, int queueSize) {
    return new KeepAliver(channel, queueSize);
  }
}
