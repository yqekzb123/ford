// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: master_log.proto

#include "master_log.pb.h"

#include <algorithm>

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/wire_format_lite.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)
#include <google/protobuf/port_def.inc>
namespace mslog_service {
class MS2MSLogRequestDefaultTypeInternal {
 public:
  ::PROTOBUF_NAMESPACE_ID::internal::ExplicitlyConstructed<MS2MSLogRequest> _instance;
} _MS2MSLogRequest_default_instance_;
class MS2MSLogResponseDefaultTypeInternal {
 public:
  ::PROTOBUF_NAMESPACE_ID::internal::ExplicitlyConstructed<MS2MSLogResponse> _instance;
} _MS2MSLogResponse_default_instance_;
}  // namespace mslog_service
static void InitDefaultsscc_info_MS2MSLogRequest_master_5flog_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::mslog_service::_MS2MSLogRequest_default_instance_;
    new (ptr) ::mslog_service::MS2MSLogRequest();
    ::PROTOBUF_NAMESPACE_ID::internal::OnShutdownDestroyMessage(ptr);
  }
  ::mslog_service::MS2MSLogRequest::InitAsDefaultInstance();
}

::PROTOBUF_NAMESPACE_ID::internal::SCCInfo<0> scc_info_MS2MSLogRequest_master_5flog_2eproto =
    {{ATOMIC_VAR_INIT(::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase::kUninitialized), 0, 0, InitDefaultsscc_info_MS2MSLogRequest_master_5flog_2eproto}, {}};

static void InitDefaultsscc_info_MS2MSLogResponse_master_5flog_2eproto() {
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  {
    void* ptr = &::mslog_service::_MS2MSLogResponse_default_instance_;
    new (ptr) ::mslog_service::MS2MSLogResponse();
    ::PROTOBUF_NAMESPACE_ID::internal::OnShutdownDestroyMessage(ptr);
  }
  ::mslog_service::MS2MSLogResponse::InitAsDefaultInstance();
}

::PROTOBUF_NAMESPACE_ID::internal::SCCInfo<0> scc_info_MS2MSLogResponse_master_5flog_2eproto =
    {{ATOMIC_VAR_INIT(::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase::kUninitialized), 0, 0, InitDefaultsscc_info_MS2MSLogResponse_master_5flog_2eproto}, {}};

static ::PROTOBUF_NAMESPACE_ID::Metadata file_level_metadata_master_5flog_2eproto[2];
static constexpr ::PROTOBUF_NAMESPACE_ID::EnumDescriptor const** file_level_enum_descriptors_master_5flog_2eproto = nullptr;
static const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* file_level_service_descriptors_master_5flog_2eproto[1];

const ::PROTOBUF_NAMESPACE_ID::uint32 TableStruct_master_5flog_2eproto::offsets[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::mslog_service::MS2MSLogRequest, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
  PROTOBUF_FIELD_OFFSET(::mslog_service::MS2MSLogRequest, ms_id_),
  PROTOBUF_FIELD_OFFSET(::mslog_service::MS2MSLogRequest, lsn_),
  ~0u,  // no _has_bits_
  PROTOBUF_FIELD_OFFSET(::mslog_service::MS2MSLogResponse, _internal_metadata_),
  ~0u,  // no _extensions_
  ~0u,  // no _oneof_case_
  ~0u,  // no _weak_field_map_
};
static const ::PROTOBUF_NAMESPACE_ID::internal::MigrationSchema schemas[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) = {
  { 0, -1, sizeof(::mslog_service::MS2MSLogRequest)},
  { 7, -1, sizeof(::mslog_service::MS2MSLogResponse)},
};

static ::PROTOBUF_NAMESPACE_ID::Message const * const file_default_instances[] = {
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::mslog_service::_MS2MSLogRequest_default_instance_),
  reinterpret_cast<const ::PROTOBUF_NAMESPACE_ID::Message*>(&::mslog_service::_MS2MSLogResponse_default_instance_),
};

const char descriptor_table_protodef_master_5flog_2eproto[] PROTOBUF_SECTION_VARIABLE(protodesc_cold) =
  "\n\020master_log.proto\022\rmslog_service\"-\n\017MS2"
  "MSLogRequest\022\r\n\005ms_id\030\001 \001(\004\022\013\n\003lsn\030\002 \001(\004"
  "\"\022\n\020MS2MSLogResponse2_\n\014MSLogService\022O\n\014"
  "WriteLSNToMS\022\036.mslog_service.MS2MSLogReq"
  "uest\032\037.mslog_service.MS2MSLogResponseB\003\200"
  "\001\001b\006proto3"
  ;
static const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable*const descriptor_table_master_5flog_2eproto_deps[1] = {
};
static ::PROTOBUF_NAMESPACE_ID::internal::SCCInfoBase*const descriptor_table_master_5flog_2eproto_sccs[2] = {
  &scc_info_MS2MSLogRequest_master_5flog_2eproto.base,
  &scc_info_MS2MSLogResponse_master_5flog_2eproto.base,
};
static ::PROTOBUF_NAMESPACE_ID::internal::once_flag descriptor_table_master_5flog_2eproto_once;
const ::PROTOBUF_NAMESPACE_ID::internal::DescriptorTable descriptor_table_master_5flog_2eproto = {
  false, false, descriptor_table_protodef_master_5flog_2eproto, "master_log.proto", 210,
  &descriptor_table_master_5flog_2eproto_once, descriptor_table_master_5flog_2eproto_sccs, descriptor_table_master_5flog_2eproto_deps, 2, 0,
  schemas, file_default_instances, TableStruct_master_5flog_2eproto::offsets,
  file_level_metadata_master_5flog_2eproto, 2, file_level_enum_descriptors_master_5flog_2eproto, file_level_service_descriptors_master_5flog_2eproto,
};

// Force running AddDescriptors() at dynamic initialization time.
static bool dynamic_init_dummy_master_5flog_2eproto = (static_cast<void>(::PROTOBUF_NAMESPACE_ID::internal::AddDescriptors(&descriptor_table_master_5flog_2eproto)), true);
namespace mslog_service {

// ===================================================================

void MS2MSLogRequest::InitAsDefaultInstance() {
}
class MS2MSLogRequest::_Internal {
 public:
};

MS2MSLogRequest::MS2MSLogRequest(::PROTOBUF_NAMESPACE_ID::Arena* arena)
  : ::PROTOBUF_NAMESPACE_ID::Message(arena) {
  SharedCtor();
  RegisterArenaDtor(arena);
  // @@protoc_insertion_point(arena_constructor:mslog_service.MS2MSLogRequest)
}
MS2MSLogRequest::MS2MSLogRequest(const MS2MSLogRequest& from)
  : ::PROTOBUF_NAMESPACE_ID::Message() {
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  ::memcpy(&ms_id_, &from.ms_id_,
    static_cast<size_t>(reinterpret_cast<char*>(&lsn_) -
    reinterpret_cast<char*>(&ms_id_)) + sizeof(lsn_));
  // @@protoc_insertion_point(copy_constructor:mslog_service.MS2MSLogRequest)
}

void MS2MSLogRequest::SharedCtor() {
  ::memset(&ms_id_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&lsn_) -
      reinterpret_cast<char*>(&ms_id_)) + sizeof(lsn_));
}

MS2MSLogRequest::~MS2MSLogRequest() {
  // @@protoc_insertion_point(destructor:mslog_service.MS2MSLogRequest)
  SharedDtor();
  _internal_metadata_.Delete<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

void MS2MSLogRequest::SharedDtor() {
  GOOGLE_DCHECK(GetArena() == nullptr);
}

void MS2MSLogRequest::ArenaDtor(void* object) {
  MS2MSLogRequest* _this = reinterpret_cast< MS2MSLogRequest* >(object);
  (void)_this;
}
void MS2MSLogRequest::RegisterArenaDtor(::PROTOBUF_NAMESPACE_ID::Arena*) {
}
void MS2MSLogRequest::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const MS2MSLogRequest& MS2MSLogRequest::default_instance() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&::scc_info_MS2MSLogRequest_master_5flog_2eproto.base);
  return *internal_default_instance();
}


void MS2MSLogRequest::Clear() {
// @@protoc_insertion_point(message_clear_start:mslog_service.MS2MSLogRequest)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  ::memset(&ms_id_, 0, static_cast<size_t>(
      reinterpret_cast<char*>(&lsn_) -
      reinterpret_cast<char*>(&ms_id_)) + sizeof(lsn_));
  _internal_metadata_.Clear<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

const char* MS2MSLogRequest::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  ::PROTOBUF_NAMESPACE_ID::Arena* arena = GetArena(); (void)arena;
  while (!ctx->Done(&ptr)) {
    ::PROTOBUF_NAMESPACE_ID::uint32 tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    CHK_(ptr);
    switch (tag >> 3) {
      // uint64 ms_id = 1;
      case 1:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 8)) {
          ms_id_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint64(&ptr);
          CHK_(ptr);
        } else goto handle_unusual;
        continue;
      // uint64 lsn = 2;
      case 2:
        if (PROTOBUF_PREDICT_TRUE(static_cast<::PROTOBUF_NAMESPACE_ID::uint8>(tag) == 16)) {
          lsn_ = ::PROTOBUF_NAMESPACE_ID::internal::ReadVarint64(&ptr);
          CHK_(ptr);
        } else goto handle_unusual;
        continue;
      default: {
      handle_unusual:
        if ((tag & 7) == 4 || tag == 0) {
          ctx->SetLastTag(tag);
          goto success;
        }
        ptr = UnknownFieldParse(tag,
            _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(),
            ptr, ctx);
        CHK_(ptr != nullptr);
        continue;
      }
    }  // switch
  }  // while
success:
  return ptr;
failure:
  ptr = nullptr;
  goto success;
#undef CHK_
}

::PROTOBUF_NAMESPACE_ID::uint8* MS2MSLogRequest::_InternalSerialize(
    ::PROTOBUF_NAMESPACE_ID::uint8* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:mslog_service.MS2MSLogRequest)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  // uint64 ms_id = 1;
  if (this->ms_id() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteUInt64ToArray(1, this->_internal_ms_id(), target);
  }

  // uint64 lsn = 2;
  if (this->lsn() != 0) {
    target = stream->EnsureSpace(target);
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::WriteUInt64ToArray(2, this->_internal_lsn(), target);
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:mslog_service.MS2MSLogRequest)
  return target;
}

size_t MS2MSLogRequest::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:mslog_service.MS2MSLogRequest)
  size_t total_size = 0;

  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  // uint64 ms_id = 1;
  if (this->ms_id() != 0) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::UInt64Size(
        this->_internal_ms_id());
  }

  // uint64 lsn = 2;
  if (this->lsn() != 0) {
    total_size += 1 +
      ::PROTOBUF_NAMESPACE_ID::internal::WireFormatLite::UInt64Size(
        this->_internal_lsn());
  }

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    return ::PROTOBUF_NAMESPACE_ID::internal::ComputeUnknownFieldsSize(
        _internal_metadata_, total_size, &_cached_size_);
  }
  int cached_size = ::PROTOBUF_NAMESPACE_ID::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void MS2MSLogRequest::MergeFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:mslog_service.MS2MSLogRequest)
  GOOGLE_DCHECK_NE(&from, this);
  const MS2MSLogRequest* source =
      ::PROTOBUF_NAMESPACE_ID::DynamicCastToGenerated<MS2MSLogRequest>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:mslog_service.MS2MSLogRequest)
    ::PROTOBUF_NAMESPACE_ID::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:mslog_service.MS2MSLogRequest)
    MergeFrom(*source);
  }
}

void MS2MSLogRequest::MergeFrom(const MS2MSLogRequest& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:mslog_service.MS2MSLogRequest)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  if (from.ms_id() != 0) {
    _internal_set_ms_id(from._internal_ms_id());
  }
  if (from.lsn() != 0) {
    _internal_set_lsn(from._internal_lsn());
  }
}

void MS2MSLogRequest::CopyFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:mslog_service.MS2MSLogRequest)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void MS2MSLogRequest::CopyFrom(const MS2MSLogRequest& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:mslog_service.MS2MSLogRequest)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool MS2MSLogRequest::IsInitialized() const {
  return true;
}

void MS2MSLogRequest::InternalSwap(MS2MSLogRequest* other) {
  using std::swap;
  _internal_metadata_.Swap<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(&other->_internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::internal::memswap<
      PROTOBUF_FIELD_OFFSET(MS2MSLogRequest, lsn_)
      + sizeof(MS2MSLogRequest::lsn_)
      - PROTOBUF_FIELD_OFFSET(MS2MSLogRequest, ms_id_)>(
          reinterpret_cast<char*>(&ms_id_),
          reinterpret_cast<char*>(&other->ms_id_));
}

::PROTOBUF_NAMESPACE_ID::Metadata MS2MSLogRequest::GetMetadata() const {
  return GetMetadataStatic();
}


// ===================================================================

void MS2MSLogResponse::InitAsDefaultInstance() {
}
class MS2MSLogResponse::_Internal {
 public:
};

MS2MSLogResponse::MS2MSLogResponse(::PROTOBUF_NAMESPACE_ID::Arena* arena)
  : ::PROTOBUF_NAMESPACE_ID::Message(arena) {
  SharedCtor();
  RegisterArenaDtor(arena);
  // @@protoc_insertion_point(arena_constructor:mslog_service.MS2MSLogResponse)
}
MS2MSLogResponse::MS2MSLogResponse(const MS2MSLogResponse& from)
  : ::PROTOBUF_NAMESPACE_ID::Message() {
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  // @@protoc_insertion_point(copy_constructor:mslog_service.MS2MSLogResponse)
}

void MS2MSLogResponse::SharedCtor() {
}

MS2MSLogResponse::~MS2MSLogResponse() {
  // @@protoc_insertion_point(destructor:mslog_service.MS2MSLogResponse)
  SharedDtor();
  _internal_metadata_.Delete<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

void MS2MSLogResponse::SharedDtor() {
  GOOGLE_DCHECK(GetArena() == nullptr);
}

void MS2MSLogResponse::ArenaDtor(void* object) {
  MS2MSLogResponse* _this = reinterpret_cast< MS2MSLogResponse* >(object);
  (void)_this;
}
void MS2MSLogResponse::RegisterArenaDtor(::PROTOBUF_NAMESPACE_ID::Arena*) {
}
void MS2MSLogResponse::SetCachedSize(int size) const {
  _cached_size_.Set(size);
}
const MS2MSLogResponse& MS2MSLogResponse::default_instance() {
  ::PROTOBUF_NAMESPACE_ID::internal::InitSCC(&::scc_info_MS2MSLogResponse_master_5flog_2eproto.base);
  return *internal_default_instance();
}


void MS2MSLogResponse::Clear() {
// @@protoc_insertion_point(message_clear_start:mslog_service.MS2MSLogResponse)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  _internal_metadata_.Clear<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>();
}

const char* MS2MSLogResponse::_InternalParse(const char* ptr, ::PROTOBUF_NAMESPACE_ID::internal::ParseContext* ctx) {
#define CHK_(x) if (PROTOBUF_PREDICT_FALSE(!(x))) goto failure
  ::PROTOBUF_NAMESPACE_ID::Arena* arena = GetArena(); (void)arena;
  while (!ctx->Done(&ptr)) {
    ::PROTOBUF_NAMESPACE_ID::uint32 tag;
    ptr = ::PROTOBUF_NAMESPACE_ID::internal::ReadTag(ptr, &tag);
    CHK_(ptr);
        if ((tag & 7) == 4 || tag == 0) {
          ctx->SetLastTag(tag);
          goto success;
        }
        ptr = UnknownFieldParse(tag,
            _internal_metadata_.mutable_unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(),
            ptr, ctx);
        CHK_(ptr != nullptr);
        continue;
  }  // while
success:
  return ptr;
failure:
  ptr = nullptr;
  goto success;
#undef CHK_
}

::PROTOBUF_NAMESPACE_ID::uint8* MS2MSLogResponse::_InternalSerialize(
    ::PROTOBUF_NAMESPACE_ID::uint8* target, ::PROTOBUF_NAMESPACE_ID::io::EpsCopyOutputStream* stream) const {
  // @@protoc_insertion_point(serialize_to_array_start:mslog_service.MS2MSLogResponse)
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    target = ::PROTOBUF_NAMESPACE_ID::internal::WireFormat::InternalSerializeUnknownFieldsToArray(
        _internal_metadata_.unknown_fields<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(::PROTOBUF_NAMESPACE_ID::UnknownFieldSet::default_instance), target, stream);
  }
  // @@protoc_insertion_point(serialize_to_array_end:mslog_service.MS2MSLogResponse)
  return target;
}

size_t MS2MSLogResponse::ByteSizeLong() const {
// @@protoc_insertion_point(message_byte_size_start:mslog_service.MS2MSLogResponse)
  size_t total_size = 0;

  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  // Prevent compiler warnings about cached_has_bits being unused
  (void) cached_has_bits;

  if (PROTOBUF_PREDICT_FALSE(_internal_metadata_.have_unknown_fields())) {
    return ::PROTOBUF_NAMESPACE_ID::internal::ComputeUnknownFieldsSize(
        _internal_metadata_, total_size, &_cached_size_);
  }
  int cached_size = ::PROTOBUF_NAMESPACE_ID::internal::ToCachedSize(total_size);
  SetCachedSize(cached_size);
  return total_size;
}

void MS2MSLogResponse::MergeFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_merge_from_start:mslog_service.MS2MSLogResponse)
  GOOGLE_DCHECK_NE(&from, this);
  const MS2MSLogResponse* source =
      ::PROTOBUF_NAMESPACE_ID::DynamicCastToGenerated<MS2MSLogResponse>(
          &from);
  if (source == nullptr) {
  // @@protoc_insertion_point(generalized_merge_from_cast_fail:mslog_service.MS2MSLogResponse)
    ::PROTOBUF_NAMESPACE_ID::internal::ReflectionOps::Merge(from, this);
  } else {
  // @@protoc_insertion_point(generalized_merge_from_cast_success:mslog_service.MS2MSLogResponse)
    MergeFrom(*source);
  }
}

void MS2MSLogResponse::MergeFrom(const MS2MSLogResponse& from) {
// @@protoc_insertion_point(class_specific_merge_from_start:mslog_service.MS2MSLogResponse)
  GOOGLE_DCHECK_NE(&from, this);
  _internal_metadata_.MergeFrom<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(from._internal_metadata_);
  ::PROTOBUF_NAMESPACE_ID::uint32 cached_has_bits = 0;
  (void) cached_has_bits;

}

void MS2MSLogResponse::CopyFrom(const ::PROTOBUF_NAMESPACE_ID::Message& from) {
// @@protoc_insertion_point(generalized_copy_from_start:mslog_service.MS2MSLogResponse)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void MS2MSLogResponse::CopyFrom(const MS2MSLogResponse& from) {
// @@protoc_insertion_point(class_specific_copy_from_start:mslog_service.MS2MSLogResponse)
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool MS2MSLogResponse::IsInitialized() const {
  return true;
}

void MS2MSLogResponse::InternalSwap(MS2MSLogResponse* other) {
  using std::swap;
  _internal_metadata_.Swap<::PROTOBUF_NAMESPACE_ID::UnknownFieldSet>(&other->_internal_metadata_);
}

::PROTOBUF_NAMESPACE_ID::Metadata MS2MSLogResponse::GetMetadata() const {
  return GetMetadataStatic();
}


// ===================================================================

MSLogService::~MSLogService() {}

const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* MSLogService::descriptor() {
  ::PROTOBUF_NAMESPACE_ID::internal::AssignDescriptors(&descriptor_table_master_5flog_2eproto);
  return file_level_service_descriptors_master_5flog_2eproto[0];
}

const ::PROTOBUF_NAMESPACE_ID::ServiceDescriptor* MSLogService::GetDescriptor() {
  return descriptor();
}

void MSLogService::WriteLSNToMS(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                         const ::mslog_service::MS2MSLogRequest*,
                         ::mslog_service::MS2MSLogResponse*,
                         ::google::protobuf::Closure* done) {
  controller->SetFailed("Method WriteLSNToMS() not implemented.");
  done->Run();
}

void MSLogService::CallMethod(const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method,
                             ::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                             const ::PROTOBUF_NAMESPACE_ID::Message* request,
                             ::PROTOBUF_NAMESPACE_ID::Message* response,
                             ::google::protobuf::Closure* done) {
  GOOGLE_DCHECK_EQ(method->service(), file_level_service_descriptors_master_5flog_2eproto[0]);
  switch(method->index()) {
    case 0:
      WriteLSNToMS(controller,
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<const ::mslog_service::MS2MSLogRequest*>(
                 request),
             ::PROTOBUF_NAMESPACE_ID::internal::DownCast<::mslog_service::MS2MSLogResponse*>(
                 response),
             done);
      break;
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      break;
  }
}

const ::PROTOBUF_NAMESPACE_ID::Message& MSLogService::GetRequestPrototype(
    const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method) const {
  GOOGLE_DCHECK_EQ(method->service(), descriptor());
  switch(method->index()) {
    case 0:
      return ::mslog_service::MS2MSLogRequest::default_instance();
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      return *::PROTOBUF_NAMESPACE_ID::MessageFactory::generated_factory()
          ->GetPrototype(method->input_type());
  }
}

const ::PROTOBUF_NAMESPACE_ID::Message& MSLogService::GetResponsePrototype(
    const ::PROTOBUF_NAMESPACE_ID::MethodDescriptor* method) const {
  GOOGLE_DCHECK_EQ(method->service(), descriptor());
  switch(method->index()) {
    case 0:
      return ::mslog_service::MS2MSLogResponse::default_instance();
    default:
      GOOGLE_LOG(FATAL) << "Bad method index; this should never happen.";
      return *::PROTOBUF_NAMESPACE_ID::MessageFactory::generated_factory()
          ->GetPrototype(method->output_type());
  }
}

MSLogService_Stub::MSLogService_Stub(::PROTOBUF_NAMESPACE_ID::RpcChannel* channel)
  : channel_(channel), owns_channel_(false) {}
MSLogService_Stub::MSLogService_Stub(
    ::PROTOBUF_NAMESPACE_ID::RpcChannel* channel,
    ::PROTOBUF_NAMESPACE_ID::Service::ChannelOwnership ownership)
  : channel_(channel),
    owns_channel_(ownership == ::PROTOBUF_NAMESPACE_ID::Service::STUB_OWNS_CHANNEL) {}
MSLogService_Stub::~MSLogService_Stub() {
  if (owns_channel_) delete channel_;
}

void MSLogService_Stub::WriteLSNToMS(::PROTOBUF_NAMESPACE_ID::RpcController* controller,
                              const ::mslog_service::MS2MSLogRequest* request,
                              ::mslog_service::MS2MSLogResponse* response,
                              ::google::protobuf::Closure* done) {
  channel_->CallMethod(descriptor()->method(0),
                       controller, request, response, done);
}

// @@protoc_insertion_point(namespace_scope)
}  // namespace mslog_service
PROTOBUF_NAMESPACE_OPEN
template<> PROTOBUF_NOINLINE ::mslog_service::MS2MSLogRequest* Arena::CreateMaybeMessage< ::mslog_service::MS2MSLogRequest >(Arena* arena) {
  return Arena::CreateMessageInternal< ::mslog_service::MS2MSLogRequest >(arena);
}
template<> PROTOBUF_NOINLINE ::mslog_service::MS2MSLogResponse* Arena::CreateMaybeMessage< ::mslog_service::MS2MSLogResponse >(Arena* arena) {
  return Arena::CreateMessageInternal< ::mslog_service::MS2MSLogResponse >(arena);
}
PROTOBUF_NAMESPACE_CLOSE

// @@protoc_insertion_point(global_scope)
#include <google/protobuf/port_undef.inc>
