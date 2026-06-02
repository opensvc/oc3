package worker

type (
	JobUpload struct {
		UploadDir string
	}
)

func (j *JobUpload) SetUploadDir(s string) {
	j.UploadDir = s
}
