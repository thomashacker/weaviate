//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package grpc

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	structpb "google.golang.org/protobuf/types/known/structpb"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type SearchRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ClassName            string            `protobuf:"bytes,1,opt,name=class_name,json=className,proto3" json:"class_name,omitempty"`
	Limit                uint32            `protobuf:"varint,2,opt,name=limit,proto3" json:"limit,omitempty"`
	AdditionalProperties []string          `protobuf:"bytes,3,rep,name=additional_properties,json=additionalProperties,proto3" json:"additional_properties,omitempty"`
	NearVector           *NearVectorParams `protobuf:"bytes,4,opt,name=near_vector,json=nearVector,proto3" json:"near_vector,omitempty"`
	NearObject           *NearObjectParams `protobuf:"bytes,5,opt,name=near_object,json=nearObject,proto3" json:"near_object,omitempty"`
	Properties           *Properties       `protobuf:"bytes,6,opt,name=properties,proto3" json:"properties,omitempty"`
}

func (x *SearchRequest) Reset() {
	*x = SearchRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SearchRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchRequest) ProtoMessage() {}

func (x *SearchRequest) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchRequest.ProtoReflect.Descriptor instead.
func (*SearchRequest) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{0}
}

func (x *SearchRequest) GetClassName() string {
	if x != nil {
		return x.ClassName
	}
	return ""
}

func (x *SearchRequest) GetLimit() uint32 {
	if x != nil {
		return x.Limit
	}
	return 0
}

func (x *SearchRequest) GetAdditionalProperties() []string {
	if x != nil {
		return x.AdditionalProperties
	}
	return nil
}

func (x *SearchRequest) GetNearVector() *NearVectorParams {
	if x != nil {
		return x.NearVector
	}
	return nil
}

func (x *SearchRequest) GetNearObject() *NearObjectParams {
	if x != nil {
		return x.NearObject
	}
	return nil
}

func (x *SearchRequest) GetProperties() *Properties {
	if x != nil {
		return x.Properties
	}
	return nil
}

type Properties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NonRefProperties []string         `protobuf:"bytes,1,rep,name=non_ref_properties,json=nonRefProperties,proto3" json:"non_ref_properties,omitempty"`
	RefProperties    []*RefProperties `protobuf:"bytes,2,rep,name=ref_properties,json=refProperties,proto3" json:"ref_properties,omitempty"`
}

func (x *Properties) Reset() {
	*x = Properties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Properties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Properties) ProtoMessage() {}

func (x *Properties) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Properties.ProtoReflect.Descriptor instead.
func (*Properties) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{1}
}

func (x *Properties) GetNonRefProperties() []string {
	if x != nil {
		return x.NonRefProperties
	}
	return nil
}

func (x *Properties) GetRefProperties() []*RefProperties {
	if x != nil {
		return x.RefProperties
	}
	return nil
}

type RefProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ClassName         string      `protobuf:"bytes,1,opt,name=class_name,json=className,proto3" json:"class_name,omitempty"`
	ReferenceProperty string      `protobuf:"bytes,2,opt,name=reference_property,json=referenceProperty,proto3" json:"reference_property,omitempty"`
	LinkedProperties  *Properties `protobuf:"bytes,3,opt,name=linked_properties,json=linkedProperties,proto3" json:"linked_properties,omitempty"`
}

func (x *RefProperties) Reset() {
	*x = RefProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RefProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RefProperties) ProtoMessage() {}

func (x *RefProperties) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RefProperties.ProtoReflect.Descriptor instead.
func (*RefProperties) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{2}
}

func (x *RefProperties) GetClassName() string {
	if x != nil {
		return x.ClassName
	}
	return ""
}

func (x *RefProperties) GetReferenceProperty() string {
	if x != nil {
		return x.ReferenceProperty
	}
	return ""
}

func (x *RefProperties) GetLinkedProperties() *Properties {
	if x != nil {
		return x.LinkedProperties
	}
	return nil
}

type NearVectorParams struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// protolint:disable:next REPEATED_FIELD_NAMES_PLURALIZED
	Vector    []float32 `protobuf:"fixed32,1,rep,packed,name=vector,proto3" json:"vector,omitempty"`
	Certainty *float64  `protobuf:"fixed64,2,opt,name=certainty,proto3,oneof" json:"certainty,omitempty"`
	Distance  *float64  `protobuf:"fixed64,3,opt,name=distance,proto3,oneof" json:"distance,omitempty"`
}

func (x *NearVectorParams) Reset() {
	*x = NearVectorParams{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NearVectorParams) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NearVectorParams) ProtoMessage() {}

func (x *NearVectorParams) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NearVectorParams.ProtoReflect.Descriptor instead.
func (*NearVectorParams) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{3}
}

func (x *NearVectorParams) GetVector() []float32 {
	if x != nil {
		return x.Vector
	}
	return nil
}

func (x *NearVectorParams) GetCertainty() float64 {
	if x != nil && x.Certainty != nil {
		return *x.Certainty
	}
	return 0
}

func (x *NearVectorParams) GetDistance() float64 {
	if x != nil && x.Distance != nil {
		return *x.Distance
	}
	return 0
}

type NearObjectParams struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id        string   `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	Certainty *float64 `protobuf:"fixed64,2,opt,name=certainty,proto3,oneof" json:"certainty,omitempty"`
	Distance  *float64 `protobuf:"fixed64,3,opt,name=distance,proto3,oneof" json:"distance,omitempty"`
}

func (x *NearObjectParams) Reset() {
	*x = NearObjectParams{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *NearObjectParams) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*NearObjectParams) ProtoMessage() {}

func (x *NearObjectParams) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use NearObjectParams.ProtoReflect.Descriptor instead.
func (*NearObjectParams) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{4}
}

func (x *NearObjectParams) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *NearObjectParams) GetCertainty() float64 {
	if x != nil && x.Certainty != nil {
		return *x.Certainty
	}
	return 0
}

func (x *NearObjectParams) GetDistance() float64 {
	if x != nil && x.Distance != nil {
		return *x.Distance
	}
	return 0
}

type SearchReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Results []*SearchResult `protobuf:"bytes,1,rep,name=results,proto3" json:"results,omitempty"`
	Took    float32         `protobuf:"fixed32,2,opt,name=took,proto3" json:"took,omitempty"`
}

func (x *SearchReply) Reset() {
	*x = SearchReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SearchReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchReply) ProtoMessage() {}

func (x *SearchReply) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchReply.ProtoReflect.Descriptor instead.
func (*SearchReply) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{5}
}

func (x *SearchReply) GetResults() []*SearchResult {
	if x != nil {
		return x.Results
	}
	return nil
}

func (x *SearchReply) GetTook() float32 {
	if x != nil {
		return x.Took
	}
	return 0
}

type SearchResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Properties           *ReturnProperties `protobuf:"bytes,1,opt,name=properties,proto3" json:"properties,omitempty"`
	AdditionalProperties *AdditionalProps  `protobuf:"bytes,2,opt,name=additional_properties,json=additionalProperties,proto3" json:"additional_properties,omitempty"`
}

func (x *SearchResult) Reset() {
	*x = SearchResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *SearchResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SearchResult) ProtoMessage() {}

func (x *SearchResult) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SearchResult.ProtoReflect.Descriptor instead.
func (*SearchResult) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{6}
}

func (x *SearchResult) GetProperties() *ReturnProperties {
	if x != nil {
		return x.Properties
	}
	return nil
}

func (x *SearchResult) GetAdditionalProperties() *AdditionalProps {
	if x != nil {
		return x.AdditionalProperties
	}
	return nil
}

type AdditionalProps struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *AdditionalProps) Reset() {
	*x = AdditionalProps{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AdditionalProps) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AdditionalProps) ProtoMessage() {}

func (x *AdditionalProps) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AdditionalProps.ProtoReflect.Descriptor instead.
func (*AdditionalProps) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{7}
}

func (x *AdditionalProps) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

type ReturnProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	NonRefProperties *structpb.Struct       `protobuf:"bytes,1,opt,name=non_ref_properties,json=nonRefProperties,proto3" json:"non_ref_properties,omitempty"`
	RefProps         []*ReturnRefProperties `protobuf:"bytes,2,rep,name=ref_props,json=refProps,proto3" json:"ref_props,omitempty"`
	ClassName        string                 `protobuf:"bytes,3,opt,name=class_name,json=className,proto3" json:"class_name,omitempty"`
}

func (x *ReturnProperties) Reset() {
	*x = ReturnProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReturnProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReturnProperties) ProtoMessage() {}

func (x *ReturnProperties) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReturnProperties.ProtoReflect.Descriptor instead.
func (*ReturnProperties) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{8}
}

func (x *ReturnProperties) GetNonRefProperties() *structpb.Struct {
	if x != nil {
		return x.NonRefProperties
	}
	return nil
}

func (x *ReturnProperties) GetRefProps() []*ReturnRefProperties {
	if x != nil {
		return x.RefProps
	}
	return nil
}

func (x *ReturnProperties) GetClassName() string {
	if x != nil {
		return x.ClassName
	}
	return ""
}

type ReturnRefProperties struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Properties *ReturnProperties `protobuf:"bytes,1,opt,name=properties,proto3" json:"properties,omitempty"`
	PropName   string            `protobuf:"bytes,2,opt,name=prop_name,json=propName,proto3" json:"prop_name,omitempty"`
}

func (x *ReturnRefProperties) Reset() {
	*x = ReturnRefProperties{}
	if protoimpl.UnsafeEnabled {
		mi := &file_weaviate_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReturnRefProperties) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReturnRefProperties) ProtoMessage() {}

func (x *ReturnRefProperties) ProtoReflect() protoreflect.Message {
	mi := &file_weaviate_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReturnRefProperties.ProtoReflect.Descriptor instead.
func (*ReturnRefProperties) Descriptor() ([]byte, []int) {
	return file_weaviate_proto_rawDescGZIP(), []int{9}
}

func (x *ReturnRefProperties) GetProperties() *ReturnProperties {
	if x != nil {
		return x.Properties
	}
	return nil
}

func (x *ReturnRefProperties) GetPropName() string {
	if x != nil {
		return x.PropName
	}
	return ""
}

var File_weaviate_proto protoreflect.FileDescriptor

var file_weaviate_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x0c, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x1a, 0x1c,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f,
	0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xb5, 0x02, 0x0a,
	0x0d, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d,
	0x0a, 0x0a, 0x63, 0x6c, 0x61, 0x73, 0x73, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x09, 0x52, 0x09, 0x63, 0x6c, 0x61, 0x73, 0x73, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x14, 0x0a,
	0x05, 0x6c, 0x69, 0x6d, 0x69, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x05, 0x6c, 0x69,
	0x6d, 0x69, 0x74, 0x12, 0x33, 0x0a, 0x15, 0x61, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61,
	0x6c, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x03, 0x20, 0x03,
	0x28, 0x09, 0x52, 0x14, 0x61, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x3f, 0x0a, 0x0b, 0x6e, 0x65, 0x61, 0x72,
	0x5f, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e, 0x2e,
	0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x4e, 0x65, 0x61,
	0x72, 0x56, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x52, 0x0a, 0x6e,
	0x65, 0x61, 0x72, 0x56, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x12, 0x3f, 0x0a, 0x0b, 0x6e, 0x65, 0x61,
	0x72, 0x5f, 0x6f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e,
	0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x4e, 0x65,
	0x61, 0x72, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x52, 0x0a,
	0x6e, 0x65, 0x61, 0x72, 0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x12, 0x38, 0x0a, 0x0a, 0x70, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x18,
	0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72,
	0x74, 0x69, 0x65, 0x73, 0x22, 0x7e, 0x0a, 0x0a, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x12, 0x2c, 0x0a, 0x12, 0x6e, 0x6f, 0x6e, 0x5f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x10,
	0x6e, 0x6f, 0x6e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73,
	0x12, 0x42, 0x0a, 0x0e, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69,
	0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65,
	0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0d, 0x72, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72,
	0x74, 0x69, 0x65, 0x73, 0x22, 0xa4, 0x01, 0x0a, 0x0d, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70,
	0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c, 0x61, 0x73, 0x73, 0x5f,
	0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63, 0x6c, 0x61, 0x73,
	0x73, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2d, 0x0a, 0x12, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e,
	0x63, 0x65, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x11, 0x72, 0x65, 0x66, 0x65, 0x72, 0x65, 0x6e, 0x63, 0x65, 0x50, 0x72, 0x6f, 0x70,
	0x65, 0x72, 0x74, 0x79, 0x12, 0x45, 0x0a, 0x11, 0x6c, 0x69, 0x6e, 0x6b, 0x65, 0x64, 0x5f, 0x70,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x18, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x50,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x10, 0x6c, 0x69, 0x6e, 0x6b, 0x65,
	0x64, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x22, 0x89, 0x01, 0x0a, 0x10,
	0x4e, 0x65, 0x61, 0x72, 0x56, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x73,
	0x12, 0x16, 0x0a, 0x06, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x03, 0x28, 0x02,
	0x52, 0x06, 0x76, 0x65, 0x63, 0x74, 0x6f, 0x72, 0x12, 0x21, 0x0a, 0x09, 0x63, 0x65, 0x72, 0x74,
	0x61, 0x69, 0x6e, 0x74, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x48, 0x00, 0x52, 0x09, 0x63,
	0x65, 0x72, 0x74, 0x61, 0x69, 0x6e, 0x74, 0x79, 0x88, 0x01, 0x01, 0x12, 0x1f, 0x0a, 0x08, 0x64,
	0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x01, 0x48, 0x01, 0x52,
	0x08, 0x64, 0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x88, 0x01, 0x01, 0x42, 0x0c, 0x0a, 0x0a,
	0x5f, 0x63, 0x65, 0x72, 0x74, 0x61, 0x69, 0x6e, 0x74, 0x79, 0x42, 0x0b, 0x0a, 0x09, 0x5f, 0x64,
	0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x22, 0x81, 0x01, 0x0a, 0x10, 0x4e, 0x65, 0x61, 0x72,
	0x4f, 0x62, 0x6a, 0x65, 0x63, 0x74, 0x50, 0x61, 0x72, 0x61, 0x6d, 0x73, 0x12, 0x0e, 0x0a, 0x02,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x12, 0x21, 0x0a, 0x09,
	0x63, 0x65, 0x72, 0x74, 0x61, 0x69, 0x6e, 0x74, 0x79, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01, 0x48,
	0x00, 0x52, 0x09, 0x63, 0x65, 0x72, 0x74, 0x61, 0x69, 0x6e, 0x74, 0x79, 0x88, 0x01, 0x01, 0x12,
	0x1f, 0x0a, 0x08, 0x64, 0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x01, 0x48, 0x01, 0x52, 0x08, 0x64, 0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x88, 0x01, 0x01,
	0x42, 0x0c, 0x0a, 0x0a, 0x5f, 0x63, 0x65, 0x72, 0x74, 0x61, 0x69, 0x6e, 0x74, 0x79, 0x42, 0x0b,
	0x0a, 0x09, 0x5f, 0x64, 0x69, 0x73, 0x74, 0x61, 0x6e, 0x63, 0x65, 0x22, 0x57, 0x0a, 0x0b, 0x53,
	0x65, 0x61, 0x72, 0x63, 0x68, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x12, 0x34, 0x0a, 0x07, 0x72, 0x65,
	0x73, 0x75, 0x6c, 0x74, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x77, 0x65,
	0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x53, 0x65, 0x61, 0x72, 0x63,
	0x68, 0x52, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x52, 0x07, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x73,
	0x12, 0x12, 0x0a, 0x04, 0x74, 0x6f, 0x6f, 0x6b, 0x18, 0x02, 0x20, 0x01, 0x28, 0x02, 0x52, 0x04,
	0x74, 0x6f, 0x6f, 0x6b, 0x22, 0xa2, 0x01, 0x0a, 0x0c, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x52,
	0x65, 0x73, 0x75, 0x6c, 0x74, 0x12, 0x3e, 0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74,
	0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x77, 0x65, 0x61, 0x76,
	0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x50,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65,
	0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x52, 0x0a, 0x15, 0x61, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f,
	0x6e, 0x61, 0x6c, 0x5f, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67,
	0x72, 0x70, 0x63, 0x2e, 0x41, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x50, 0x72,
	0x6f, 0x70, 0x73, 0x52, 0x14, 0x61, 0x64, 0x64, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x50,
	0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x22, 0x21, 0x0a, 0x0f, 0x41, 0x64, 0x64,
	0x69, 0x74, 0x69, 0x6f, 0x6e, 0x61, 0x6c, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x12, 0x0e, 0x0a, 0x02,
	0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x22, 0xb8, 0x01, 0x0a,
	0x10, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65,
	0x73, 0x12, 0x45, 0x0a, 0x12, 0x6e, 0x6f, 0x6e, 0x5f, 0x72, 0x65, 0x66, 0x5f, 0x70, 0x72, 0x6f,
	0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e,
	0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x52, 0x10, 0x6e, 0x6f, 0x6e, 0x52, 0x65, 0x66, 0x50, 0x72,
	0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x3e, 0x0a, 0x09, 0x72, 0x65, 0x66, 0x5f,
	0x70, 0x72, 0x6f, 0x70, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x21, 0x2e, 0x77, 0x65,
	0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x52, 0x65, 0x74, 0x75, 0x72,
	0x6e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x52, 0x08,
	0x72, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x73, 0x12, 0x1d, 0x0a, 0x0a, 0x63, 0x6c, 0x61, 0x73,
	0x73, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x63, 0x6c,
	0x61, 0x73, 0x73, 0x4e, 0x61, 0x6d, 0x65, 0x22, 0x72, 0x0a, 0x13, 0x52, 0x65, 0x74, 0x75, 0x72,
	0x6e, 0x52, 0x65, 0x66, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x3e,
	0x0a, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70,
	0x63, 0x2e, 0x52, 0x65, 0x74, 0x75, 0x72, 0x6e, 0x50, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69,
	0x65, 0x73, 0x52, 0x0a, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x74, 0x69, 0x65, 0x73, 0x12, 0x1b,
	0x0a, 0x09, 0x70, 0x72, 0x6f, 0x70, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x70, 0x72, 0x6f, 0x70, 0x4e, 0x61, 0x6d, 0x65, 0x32, 0x4e, 0x0a, 0x08, 0x57,
	0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x12, 0x42, 0x0a, 0x06, 0x53, 0x65, 0x61, 0x72, 0x63,
	0x68, 0x12, 0x1b, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63,
	0x2e, 0x53, 0x65, 0x61, 0x72, 0x63, 0x68, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x19,
	0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x67, 0x72, 0x70, 0x63, 0x2e, 0x53, 0x65,
	0x61, 0x72, 0x63, 0x68, 0x52, 0x65, 0x70, 0x6c, 0x79, 0x22, 0x00, 0x42, 0x23, 0x5a, 0x21, 0x67,
	0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61,
	0x74, 0x65, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x67, 0x72, 0x70, 0x63,
	0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_weaviate_proto_rawDescOnce sync.Once
	file_weaviate_proto_rawDescData = file_weaviate_proto_rawDesc
)

func file_weaviate_proto_rawDescGZIP() []byte {
	file_weaviate_proto_rawDescOnce.Do(func() {
		file_weaviate_proto_rawDescData = protoimpl.X.CompressGZIP(file_weaviate_proto_rawDescData)
	})
	return file_weaviate_proto_rawDescData
}

var (
	file_weaviate_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
	file_weaviate_proto_goTypes  = []interface{}{
		(*SearchRequest)(nil),       // 0: weaviategrpc.SearchRequest
		(*Properties)(nil),          // 1: weaviategrpc.Properties
		(*RefProperties)(nil),       // 2: weaviategrpc.RefProperties
		(*NearVectorParams)(nil),    // 3: weaviategrpc.NearVectorParams
		(*NearObjectParams)(nil),    // 4: weaviategrpc.NearObjectParams
		(*SearchReply)(nil),         // 5: weaviategrpc.SearchReply
		(*SearchResult)(nil),        // 6: weaviategrpc.SearchResult
		(*AdditionalProps)(nil),     // 7: weaviategrpc.AdditionalProps
		(*ReturnProperties)(nil),    // 8: weaviategrpc.ReturnProperties
		(*ReturnRefProperties)(nil), // 9: weaviategrpc.ReturnRefProperties
		(*structpb.Struct)(nil),     // 10: google.protobuf.Struct
	}
)

var file_weaviate_proto_depIdxs = []int32{
	3,  // 0: weaviategrpc.SearchRequest.near_vector:type_name -> weaviategrpc.NearVectorParams
	4,  // 1: weaviategrpc.SearchRequest.near_object:type_name -> weaviategrpc.NearObjectParams
	1,  // 2: weaviategrpc.SearchRequest.properties:type_name -> weaviategrpc.Properties
	2,  // 3: weaviategrpc.Properties.ref_properties:type_name -> weaviategrpc.RefProperties
	1,  // 4: weaviategrpc.RefProperties.linked_properties:type_name -> weaviategrpc.Properties
	6,  // 5: weaviategrpc.SearchReply.results:type_name -> weaviategrpc.SearchResult
	8,  // 6: weaviategrpc.SearchResult.properties:type_name -> weaviategrpc.ReturnProperties
	7,  // 7: weaviategrpc.SearchResult.additional_properties:type_name -> weaviategrpc.AdditionalProps
	10, // 8: weaviategrpc.ReturnProperties.non_ref_properties:type_name -> google.protobuf.Struct
	9,  // 9: weaviategrpc.ReturnProperties.ref_props:type_name -> weaviategrpc.ReturnRefProperties
	8,  // 10: weaviategrpc.ReturnRefProperties.properties:type_name -> weaviategrpc.ReturnProperties
	0,  // 11: weaviategrpc.Weaviate.Search:input_type -> weaviategrpc.SearchRequest
	5,  // 12: weaviategrpc.Weaviate.Search:output_type -> weaviategrpc.SearchReply
	12, // [12:13] is the sub-list for method output_type
	11, // [11:12] is the sub-list for method input_type
	11, // [11:11] is the sub-list for extension type_name
	11, // [11:11] is the sub-list for extension extendee
	0,  // [0:11] is the sub-list for field type_name
}

func init() { file_weaviate_proto_init() }
func file_weaviate_proto_init() {
	if File_weaviate_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_weaviate_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SearchRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Properties); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RefProperties); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NearVectorParams); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*NearObjectParams); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SearchReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*SearchResult); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AdditionalProps); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReturnProperties); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_weaviate_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReturnRefProperties); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_weaviate_proto_msgTypes[3].OneofWrappers = []interface{}{}
	file_weaviate_proto_msgTypes[4].OneofWrappers = []interface{}{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_weaviate_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_weaviate_proto_goTypes,
		DependencyIndexes: file_weaviate_proto_depIdxs,
		MessageInfos:      file_weaviate_proto_msgTypes,
	}.Build()
	File_weaviate_proto = out.File
	file_weaviate_proto_rawDesc = nil
	file_weaviate_proto_goTypes = nil
	file_weaviate_proto_depIdxs = nil
}
