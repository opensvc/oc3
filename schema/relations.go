package schema

func init() {
	AppsPublicationsAppID.Ref = AppsID
	AppsPublicationsGroupID.Ref = AuthGroupID
	AppsResponsiblesAppID.Ref = AppsID
	AppsResponsiblesGroupID.Ref = AuthGroupID

	SvcdisksDiskID.Ref = DiskinfoDiskID
	SvcdisksNodeID.Ref = NodesNodeID
	SvcdisksSvcID.Ref = ServicesSvcID

	ServicesSvcApp.Ref = AppsApp

	NodeIPNodeID.Ref = NodesNodeID

	SvcmonSvcID.Ref = ServicesSvcID
	SvcmonLogSvcID.Ref = ServicesSvcID
}
