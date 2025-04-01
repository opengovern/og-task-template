package task

type ArtifactSbom struct {
	ID string
}

func (r ArtifactSbom) UniqueID() string {
	return r.ID
}
