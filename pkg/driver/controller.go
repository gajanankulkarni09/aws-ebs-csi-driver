/*
Copyright 2019 The Kubernetes Authors.

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

package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	csi "github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/golang/protobuf/ptypes"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/cloud"
	"github.com/kubernetes-sigs/aws-ebs-csi-driver/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/klog"
)

var (
	// volumeCaps represents how the volume could be accessed.
	// It is SINGLE_NODE_WRITER since EBS volume could only be
	// attached to a single node at any given time.
	volumeCaps = []csi.VolumeCapability_AccessMode{
		{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}

	// controllerCaps represents the capability of controller service
	controllerCaps = []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_PUBLISH_UNPUBLISH_VOLUME,
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT,
		csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS,
		csi.ControllerServiceCapability_RPC_EXPAND_VOLUME,
	}
)
var (
	// ErrInvalidAccounts is an error that is returned when wrong
	// format of aws-accounts is given in aws-accounts.yaml file
	ErrInvalidAccounts = errors.New("invalid aws-accounts secret provided")

	errAwsCrenditailNotExist = errors.New("credentials for current account not provided")

	errInvalidIDFormat = errors.New("invalid id format for multiple aws account case")
)

// controllerService represents the controller service of CSI driver
type controllerService struct {
	cloud                  cloud.Cloud
	driverOptions          *DriverOptions
	cloudMap               map[string]cloud.Cloud
	multipleAccountSupport bool
}

var (
	// NewMetadataFunc is a variable for the cloud.NewMetadata function that can
	// be overwritten in unit tests.
	NewMetadataFunc = cloud.NewMetadata
	// NewCloudFunc is a variable for the cloud.NewCloud function that can
	// be overwritten in unit tests.
	NewCloudFunc = cloud.NewCloud
	// NewCloudWithAwsProfileFunc is a variable for the cloud.NewCloudWithAwsProfile function that can
	// be overwritten in unit tests.
	NewCloudWithAwsProfileFunc = cloud.NewCloudWithAwsProfile
)

// newControllerService creates a new controller service
// it panics if failed to create the service
func newControllerService(driverOptions *DriverOptions) controllerService {
	region := os.Getenv("AWS_REGION")
	if region == "" {
		metadata, err := NewMetadataFunc()
		if err != nil {
			panic(err)
		}
		region = metadata.GetRegion()
	}

	multipleAccountsSupport := false
	var err error
	if multipleAccountsSupport, err = strconv.ParseBool(os.Getenv("AWS_CROSS_ACCOUNT_SUPPORT")); err != nil {
		multipleAccountsSupport = false
	}

	if multipleAccountsSupport {
		awsProfiles, err := getAwsProfiles()
		if err != nil {
			panic(err)
		}

		cloudMap := make(map[string]cloud.Cloud)
		for _, awsProfile := range awsProfiles {
			cloud, err := NewCloudWithAwsProfileFunc(region, awsProfile)
			if err != nil {
				panic(err)
			}
			cloudMap[awsProfile] = cloud
		}
		return controllerService{
			cloudMap:               cloudMap,
			driverOptions:          driverOptions,
			multipleAccountSupport: true,
		}

	}

	//case if multipleAccountSUpport is not required
	cloud, err := NewCloudFunc(region)
	if err != nil {
		panic(err)
	}

	return controllerService{
		cloud:         cloud,
		driverOptions: driverOptions,
	}

}

func (d *controllerService) CreateVolume(ctx context.Context, req *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	klog.V(4).Infof("CreateVolume: called with args %+v", *req)
	klog.V(4).Infof("CreateVolume: called with args (changed gajanan changes) %+v", *req)
	volName := req.GetName()
	if len(volName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume name not provided")
	}

	volSizeBytes, err := getVolSizeBytes(req)
	if err != nil {
		return nil, err
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if !isValidVolumeCapabilities(volCaps) {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not supported")
	}

	var accountID string
	if d.multipleAccountSupport {
		if accountID = pickAccountID(req.GetAccessibilityRequirements()); accountID == "" {
			return nil, status.Error(codes.InvalidArgument, "AccountID must be provided in topology requirement in request for muliple account case")
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	disk, err := reqCloud.GetDiskByName(ctx, volName, volSizeBytes)
	if err != nil {
		switch err {
		case cloud.ErrNotFound:
		case cloud.ErrMultiDisks:
			return nil, status.Error(codes.Internal, err.Error())
		case cloud.ErrDiskExistsDiffSize:
			return nil, status.Error(codes.AlreadyExists, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	var (
		volumeType  string
		iopsPerGB   int
		isEncrypted bool
		kmsKeyID    string
		volumeTags  = map[string]string{
			cloud.VolumeNameTagKey: volName,
		}
	)

	for key, value := range req.GetParameters() {
		switch strings.ToLower(key) {
		case "fstype":
			klog.Warning("\"fstype\" is deprecated, please use \"csi.storage.k8s.io/fstype\" instead")
		case VolumeTypeKey:
			volumeType = value
		case IopsPerGBKey:
			iopsPerGB, err = strconv.Atoi(value)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "Could not parse invalid iopsPerGB: %v", err)
			}
		case EncryptedKey:
			if value == "true" {
				isEncrypted = true
			} else {
				isEncrypted = false
			}
		case KmsKeyIDKey:
			kmsKeyID = value
		case PVCNameKey:
			volumeTags[PVCNameTag] = value
		case PVCNamespaceKey:
			volumeTags[PVCNamespaceTag] = value
		case PVNameKey:
			volumeTags[PVNameTag] = value
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Invalid parameter key %s for CreateVolume", key)
		}
	}

	snapshotID := ""
	volumeSource := req.GetVolumeContentSource()
	if volumeSource != nil {
		if _, ok := volumeSource.GetType().(*csi.VolumeContentSource_Snapshot); !ok {
			return nil, status.Error(codes.InvalidArgument, "Unsupported volumeContentSource type")
		}
		sourceSnapshot := volumeSource.GetSnapshot()
		if sourceSnapshot == nil {
			return nil, status.Error(codes.InvalidArgument, "Error retrieving snapshot from the volumeContentSource")
		}
		snapshotID = sourceSnapshot.GetSnapshotId()
	}

	// volume exists already
	if disk != nil {
		if disk.SnapshotID != snapshotID {
			return nil, status.Errorf(codes.AlreadyExists, "Volume already exists, but was restored from a different snapshot than %s", snapshotID)
		}
		return newCreateVolumeResponse(disk, accountID), nil
	}

	// create a new volume
	zone := pickAvailabilityZone(req.GetAccessibilityRequirements())

	// fill volume tags
	if d.driverOptions.kubernetesClusterID != "" {
		resourceLifecycleTag := ResourceLifecycleTagPrefix + d.driverOptions.kubernetesClusterID
		volumeTags[resourceLifecycleTag] = ResourceLifecycleOwned
		volumeTags[NameTag] = d.driverOptions.kubernetesClusterID + "-dynamic-" + volName
	}
	for k, v := range d.driverOptions.extraVolumeTags {
		volumeTags[k] = v
	}

	if d.multipleAccountSupport && strings.Contains(snapshotID, accountID) {
		snapshotID, _, _ = extractIDs(snapshotID)
	}

	opts := &cloud.DiskOptions{
		CapacityBytes:    volSizeBytes,
		Tags:             volumeTags,
		VolumeType:       volumeType,
		IOPSPerGB:        iopsPerGB,
		AvailabilityZone: zone,
		Encrypted:        isEncrypted,
		KmsKeyID:         kmsKeyID,
		SnapshotID:       snapshotID,
	}

	disk, err = reqCloud.CreateDisk(ctx, volName, opts)
	if err != nil {
		errCode := codes.Internal
		if err == cloud.ErrNotFound {
			errCode = codes.NotFound
		}
		return nil, status.Errorf(errCode, "Could not create volume %q: %v", volName, err)
	}

	return newCreateVolumeResponse(disk, accountID), nil
}

func (d *controllerService) DeleteVolume(ctx context.Context, req *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	klog.V(4).Infof("DeleteVolume: called with args: %+v", *req)
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	var accountID string
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	if _, err := reqCloud.DeleteDisk(ctx, volumeID); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(4).Info("DeleteVolume: volume not found, returning with success")
			return &csi.DeleteVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Could not delete volume ID %q: %v", volumeID, err)
	}

	return &csi.DeleteVolumeResponse{}, nil
}

func (d *controllerService) ControllerPublishVolume(ctx context.Context, req *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	klog.V(4).Infof("ControllerPublishVolume: called with args %+v", *req)
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	var accountID string
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}

	volCap := req.GetVolumeCapability()
	if volCap == nil {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not provided")
	}

	caps := []*csi.VolumeCapability{volCap}
	if !isValidVolumeCapabilities(caps) {
		return nil, status.Error(codes.InvalidArgument, "Volume capability not supported")
	}

	if !reqCloud.IsExistInstance(ctx, nodeID) {
		return nil, status.Errorf(codes.NotFound, "Instance %q not found", nodeID)
	}

	if _, err := reqCloud.GetDiskByID(ctx, volumeID); err != nil {
		if err == cloud.ErrNotFound {
			return nil, status.Error(codes.NotFound, "Volume not found")
		}
		return nil, status.Errorf(codes.Internal, "Could not get volume with ID %q: %v", volumeID, err)
	}

	devicePath, err := reqCloud.AttachDisk(ctx, volumeID, nodeID)
	if err != nil {
		if err == cloud.ErrAlreadyExists {
			return nil, status.Error(codes.AlreadyExists, err.Error())
		}
		return nil, status.Errorf(codes.Internal, "Could not attach volume %q to node %q: %v", volumeID, nodeID, err)
	}
	klog.V(5).Infof("ControllerPublishVolume: volume %s attached to node %s through device %s", volumeID, nodeID, devicePath)

	pvInfo := map[string]string{DevicePathKey: devicePath}
	if d.multipleAccountSupport {
		pvInfo[AccountIDKey] = accountID
	}
	return &csi.ControllerPublishVolumeResponse{PublishContext: pvInfo}, nil
}

func (d *controllerService) ControllerUnpublishVolume(ctx context.Context, req *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	klog.V(4).Infof("ControllerUnpublishVolume: called with args %+v", *req)
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	var accountID string
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	nodeID := req.GetNodeId()
	if len(nodeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Node ID not provided")
	}

	if err := reqCloud.DetachDisk(ctx, volumeID, nodeID); err != nil {
		if err == cloud.ErrNotFound {
			return &csi.ControllerUnpublishVolumeResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Could not detach volume %q from node %q: %v", volumeID, nodeID, err)
	}
	klog.V(5).Infof("ControllerUnpublishVolume: volume %s detached from node %s", volumeID, nodeID)

	return &csi.ControllerUnpublishVolumeResponse{}, nil
}

func (d *controllerService) ControllerGetCapabilities(ctx context.Context, req *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	klog.V(4).Infof("ControllerGetCapabilities: called with args %+v", *req)
	var caps []*csi.ControllerServiceCapability
	for _, cap := range controllerCaps {
		c := &csi.ControllerServiceCapability{
			Type: &csi.ControllerServiceCapability_Rpc{
				Rpc: &csi.ControllerServiceCapability_RPC{
					Type: cap,
				},
			},
		}
		caps = append(caps, c)
	}
	return &csi.ControllerGetCapabilitiesResponse{Capabilities: caps}, nil
}

func (d *controllerService) GetCapacity(ctx context.Context, req *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	klog.V(4).Infof("GetCapacity: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ListVolumes(ctx context.Context, req *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	klog.V(4).Infof("ListVolumes: called with args %+v", *req)
	return nil, status.Error(codes.Unimplemented, "")
}

func (d *controllerService) ValidateVolumeCapabilities(ctx context.Context, req *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	klog.V(4).Infof("ValidateVolumeCapabilities: called with args %+v", *req)
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	var accountID string
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	volCaps := req.GetVolumeCapabilities()
	if len(volCaps) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume capabilities not provided")
	}

	if _, err := reqCloud.GetDiskByID(ctx, volumeID); err != nil {
		if err == cloud.ErrNotFound {
			return nil, status.Error(codes.NotFound, "Volume not found")
		}
		return nil, status.Errorf(codes.Internal, "Could not get volume with ID %q: %v", volumeID, err)
	}

	var confirmed *csi.ValidateVolumeCapabilitiesResponse_Confirmed
	if isValidVolumeCapabilities(volCaps) {
		confirmed = &csi.ValidateVolumeCapabilitiesResponse_Confirmed{VolumeCapabilities: volCaps}
	}
	return &csi.ValidateVolumeCapabilitiesResponse{
		Confirmed: confirmed,
	}, nil
}

func (d *controllerService) ControllerExpandVolume(ctx context.Context, req *csi.ControllerExpandVolumeRequest) (*csi.ControllerExpandVolumeResponse, error) {
	klog.V(4).Infof("ControllerExpandVolume: called with args %+v", *req)
	volumeID := req.GetVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Volume ID not provided")
	}

	var accountID string
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	capRange := req.GetCapacityRange()
	if capRange == nil {
		return nil, status.Error(codes.InvalidArgument, "Capacity range not provided")
	}

	newSize := util.RoundUpBytes(capRange.GetRequiredBytes())
	maxVolSize := capRange.GetLimitBytes()
	if maxVolSize > 0 && maxVolSize < newSize {
		return nil, status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
	}

	actualSizeGiB, err := reqCloud.ResizeDisk(ctx, volumeID, newSize)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not resize volume %q: %v", volumeID, err)
	}

	return &csi.ControllerExpandVolumeResponse{
		CapacityBytes:         util.GiBToBytes(actualSizeGiB),
		NodeExpansionRequired: true,
	}, nil
}

func isValidVolumeCapabilities(volCaps []*csi.VolumeCapability) bool {
	hasSupport := func(cap *csi.VolumeCapability) bool {
		for _, c := range volumeCaps {
			if c.GetMode() == cap.AccessMode.GetMode() {
				return true
			}
		}
		return false
	}

	foundAll := true
	for _, c := range volCaps {
		if !hasSupport(c) {
			foundAll = false
		}
	}
	return foundAll
}

func (d *controllerService) CreateSnapshot(ctx context.Context, req *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	klog.V(4).Infof("CreateSnapshot: called with args %+v", req)
	snapshotName := req.GetName()
	if len(snapshotName) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot name not provided")
	}

	volumeID := req.GetSourceVolumeId()
	if len(volumeID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot volume source ID not provided")
	}

	var accountID string = ""
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not in mentioned secrets file", accountID)
	}

	snapshot, err := reqCloud.GetSnapshotByName(ctx, snapshotName)
	if err != nil && err != cloud.ErrNotFound {
		klog.Errorf("Error looking for the snapshot %s: %v", snapshotName, err)
		return nil, err
	}
	if snapshot != nil {
		if snapshot.SourceVolumeID != volumeID {
			return nil, status.Errorf(codes.AlreadyExists, "Snapshot %s already exists for different volume (%s)", snapshotName, snapshot.SourceVolumeID)
		}
		klog.V(4).Infof("Snapshot %s of volume %s already exists; nothing to do", snapshotName, volumeID)
		return newCreateSnapshotResponse(snapshot, accountID)
	}
	opts := &cloud.SnapshotOptions{
		Tags: map[string]string{cloud.SnapshotNameTagKey: snapshotName},
	}
	snapshot, err = reqCloud.CreateSnapshot(ctx, volumeID, opts)

	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not create snapshot %q: %v", snapshotName, err)
	}
	createSnapshotResponse, err := newCreateSnapshotResponse(snapshot, accountID)
	return createSnapshotResponse, err
}

func (d *controllerService) DeleteSnapshot(ctx context.Context, req *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	klog.V(4).Infof("DeleteSnapshot: called with args %+v", req)
	snapshotID := req.GetSnapshotId()
	if len(snapshotID) == 0 {
		return nil, status.Error(codes.InvalidArgument, "Snapshot ID not provided")
	}

	var accountID string
	var err error
	if d.multipleAccountSupport {
		if snapshotID, accountID, err = extractIDs(snapshotID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "snapshot id (%s) format is invalid for multiple account case", snapshotID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned in secrets file", accountID)
	}

	if _, err := reqCloud.DeleteSnapshot(ctx, snapshotID); err != nil {
		if err == cloud.ErrNotFound {
			klog.V(4).Info("DeleteSnapshot: snapshot not found, returning with success")
			return &csi.DeleteSnapshotResponse{}, nil
		}
		return nil, status.Errorf(codes.Internal, "Could not delete snapshot ID %q: %v", snapshotID, err)
	}

	return &csi.DeleteSnapshotResponse{}, nil
}

func (d *controllerService) ListSnapshots(ctx context.Context, req *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	klog.V(4).Infof("ListSnapshots: called with args %+v", req)
	var snapshots []*cloud.Snapshot

	snapshotID := req.GetSnapshotId()

	if len(snapshotID) != 0 {
		accountID := ""
		var err error
		if d.multipleAccountSupport {
			if snapshotID, accountID, err = extractIDs(snapshotID); err == errInvalidIDFormat {
				return nil, status.Errorf(codes.InvalidArgument, "snapshot id (%s) format is invalid for multiple account case", snapshotID)
			}
		}

		var reqCloud cloud.Cloud
		if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
			return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned secrets file", snapshotID)
		}

		snapshot, err := reqCloud.GetSnapshotByID(ctx, snapshotID)
		if err != nil {
			if err == cloud.ErrNotFound {
				klog.V(4).Info("ListSnapshots: snapshot not found, returning with success")
				return &csi.ListSnapshotsResponse{}, nil
			}
			return nil, status.Errorf(codes.Internal, "Could not get snapshot ID %q: %v", snapshotID, err)
		}
		snapshots = append(snapshots, snapshot)
		if response, err := newListSnapshotsResponse(&cloud.ListSnapshotsResponse{
			Snapshots: snapshots,
		}, accountID); err != nil {
			return nil, status.Errorf(codes.Internal, "Could not build ListSnapshotsResponse: %v", err)
		} else {
			return response, nil
		}
	}

	volumeID := req.GetSourceVolumeId()
	nextToken := req.GetStartingToken()
	maxEntries := int64(req.GetMaxEntries())

	var accountID string = ""
	var err error
	if d.multipleAccountSupport {
		if volumeID, accountID, err = extractIDs(volumeID); err == errInvalidIDFormat {
			return nil, status.Errorf(codes.InvalidArgument, "volume id (%s) format is invalid for multiple account case", volumeID)
		}
	}

	var reqCloud cloud.Cloud
	if reqCloud, err = d.getCloudInstance(accountID); err == errAwsCrenditailNotExist {
		return nil, status.Errorf(codes.ResourceExhausted, "secret credentials for account id %s not mentioned secrets file", accountID)
	}

	cloudSnapshots, err := reqCloud.ListSnapshots(ctx, volumeID, maxEntries, nextToken)
	if err != nil {
		if err == cloud.ErrNotFound {
			klog.V(4).Info("ListSnapshots: snapshot not found, returning with success")
			return &csi.ListSnapshotsResponse{}, nil
		}
		if err == cloud.ErrInvalidMaxResults {
			return nil, status.Errorf(codes.InvalidArgument, "Error mapping MaxEntries to AWS MaxResults: %v", err)
		}
		return nil, status.Errorf(codes.Internal, "Could not list snapshots: %v", err)
	}

	response, err := newListSnapshotsResponse(cloudSnapshots, accountID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Could not build ListSnapshotsResponse: %v", err)
	}
	return response, nil
}

func extractIDs(csiVolumeID string) (volumeID string, accountID string, err error) {

	segments := strings.Split(csiVolumeID, "_")
	if len(segments) != 2 {
		return "", "", errInvalidIDFormat
	}

	accountID = segments[0]
	volumeID = segments[1]

	return volumeID, accountID, nil

}

func (d *controllerService) getCloudInstance(awsAccountID string) (cloud cloud.Cloud, err error) {
	if d.multipleAccountSupport {
		awsProfile := fmt.Sprintf("account_%s", awsAccountID)
		if tempCloud, ok := d.cloudMap[awsProfile]; ok == true {
			return tempCloud, nil
		}

		return nil, errAwsCrenditailNotExist
	}

	return d.cloud, nil
}

func getAwsProfiles() ([]string, error) {

	success := true
	if os.Getenv("ACCOUNTS_COUNT") != "" {
		if accountsCount, err := strconv.Atoi(os.Getenv("ACCOUNTS_COUNT")); err == nil {
			awsProfiles := make([]string, accountsCount)
			for i := 0; i < accountsCount; i++ {
				if accountID := os.Getenv(fmt.Sprintf("ACCOUNTID_%d", i+1)); accountID != "" {
					awsProfiles[i] = fmt.Sprintf("account_%s", accountID)
				} else {
					success = false
					break
				}
			}
			if success {
				return awsProfiles, nil
			}
		}
	}
	return nil, ErrInvalidAccounts
}

// pickAvailabilityZone selects 1 zone given topology requirement.
// if not found, empty string is returned.
func pickAvailabilityZone(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		zone, exists := topology.GetSegments()[TopologyKey]
		if exists {
			return zone
		}
	}
	for _, topology := range requirement.GetRequisite() {
		zone, exists := topology.GetSegments()[TopologyKey]
		if exists {
			return zone
		}
	}
	return ""
}

func pickAccountID(requirement *csi.TopologyRequirement) string {
	if requirement == nil {
		return ""
	}
	for _, topology := range requirement.GetPreferred() {
		accountID, exists := topology.GetSegments()[TopologyAccountIDKey]
		if exists {
			return accountID
		}
	}
	for _, topology := range requirement.GetRequisite() {
		accountID, exists := topology.GetSegments()[TopologyAccountIDKey]
		if exists {
			return accountID
		}
	}
	return ""
}

func newCreateVolumeResponse(disk *cloud.Disk, accountID string) *csi.CreateVolumeResponse {
	var src *csi.VolumeContentSource
	if disk.SnapshotID != "" {
		src = &csi.VolumeContentSource{
			Type: &csi.VolumeContentSource_Snapshot{
				Snapshot: &csi.VolumeContentSource_SnapshotSource{
					SnapshotId: disk.SnapshotID,
				},
			},
		}
	}

	if accountID == "" {
		return &csi.CreateVolumeResponse{
			Volume: &csi.Volume{
				VolumeId:      disk.VolumeID,
				CapacityBytes: util.GiBToBytes(disk.CapacityGiB),
				VolumeContext: map[string]string{},
				AccessibleTopology: []*csi.Topology{
					{
						Segments: map[string]string{TopologyKey: disk.AvailabilityZone},
					},
				},
				ContentSource: src,
			},
		}
	}

	return &csi.CreateVolumeResponse{
		Volume: &csi.Volume{
			VolumeId:      fmt.Sprintf("%s_%s", accountID, disk.VolumeID),
			CapacityBytes: util.GiBToBytes(disk.CapacityGiB),
			VolumeContext: map[string]string{},
			AccessibleTopology: []*csi.Topology{
				{
					Segments: map[string]string{TopologyKey: disk.AvailabilityZone,
						TopologyAccountIDKey: accountID},
				},
			},
			ContentSource: src,
		},
	}

}

func newCreateSnapshotResponse(snapshot *cloud.Snapshot, accountID string) (*csi.CreateSnapshotResponse, error) {
	ts, err := ptypes.TimestampProto(snapshot.CreationTime)
	if err != nil {
		return nil, err
	}

	snapshotID := snapshot.SnapshotID
	if accountID != "" {
		snapshotID = fmt.Sprintf("%s_%s", accountID, snapshotID)
	}

	return &csi.CreateSnapshotResponse{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: snapshot.SourceVolumeID,
			SizeBytes:      snapshot.Size,
			CreationTime:   ts,
			ReadyToUse:     snapshot.ReadyToUse,
		},
	}, nil
}

func newListSnapshotsResponse(cloudResponse *cloud.ListSnapshotsResponse, accountID string) (*csi.ListSnapshotsResponse, error) {

	var entries []*csi.ListSnapshotsResponse_Entry
	for _, snapshot := range cloudResponse.Snapshots {
		snapshotResponseEntry, err := newListSnapshotsResponseEntry(snapshot, accountID)
		if err != nil {
			return nil, err
		}
		entries = append(entries, snapshotResponseEntry)
	}
	return &csi.ListSnapshotsResponse{
		Entries:   entries,
		NextToken: cloudResponse.NextToken,
	}, nil
}

func newListSnapshotsResponseEntry(snapshot *cloud.Snapshot, accountID string) (*csi.ListSnapshotsResponse_Entry, error) {
	ts, err := ptypes.TimestampProto(snapshot.CreationTime)
	if err != nil {
		return nil, err
	}

	snapshotID := snapshot.SnapshotID
	if accountID != "" {
		snapshotID = fmt.Sprintf("%s_%s", accountID, snapshotID)
	}
	return &csi.ListSnapshotsResponse_Entry{
		Snapshot: &csi.Snapshot{
			SnapshotId:     snapshotID,
			SourceVolumeId: snapshot.SourceVolumeID,
			SizeBytes:      snapshot.Size,
			CreationTime:   ts,
			ReadyToUse:     snapshot.ReadyToUse,
		},
	}, nil
}

func getVolSizeBytes(req *csi.CreateVolumeRequest) (int64, error) {
	var volSizeBytes int64
	capRange := req.GetCapacityRange()
	if capRange == nil {
		volSizeBytes = cloud.DefaultVolumeSize
	} else {
		volSizeBytes = util.RoundUpBytes(capRange.GetRequiredBytes())
		maxVolSize := capRange.GetLimitBytes()
		if maxVolSize > 0 && maxVolSize < volSizeBytes {
			return 0, status.Error(codes.InvalidArgument, "After round-up, volume size exceeds the limit specified")
		}
	}
	return volSizeBytes, nil
}
