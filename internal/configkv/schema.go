package configkv

type Field struct {
	Type     string
	Min, Max float64
	HasRange bool
	Default  any
	Reload   string
}

var DocServiceSchema = map[string]Field{
	"max_pdf_bytes":                 {Type: "int", Min: 1048576, Max: 104857600, HasRange: true, Default: int64(26214400), Reload: "hot"},
	"presign_upload_ttl_seconds":    {Type: "int", Min: 60, Max: 3600, HasRange: true, Default: int64(900), Reload: "hot"},
	"presign_download_ttl_seconds":  {Type: "int", Min: 60, Max: 900, HasRange: true, Default: int64(300), Reload: "hot"},
	"reconcile_interval_minutes":    {Type: "int", Min: 1, Max: 60, HasRange: true, Default: int64(10), Reload: "hot"},
	"reconcile_stuck_after_minutes": {Type: "int", Min: 5, Max: 720, HasRange: true, Default: int64(30), Reload: "hot"},
	"reconcile_dry_run":             {Type: "bool", HasRange: false, Default: false, Reload: "hot"},
	"doc_upload_grace_minutes":      {Type: "int", Min: 5, Max: 240, HasRange: true, Default: int64(30), Reload: "hot"},
	"min_safety_age_minutes":        {Type: "int", Min: 1, Max: 120, HasRange: true, Default: int64(15), Reload: "hot"},
}

func Defaults() map[string]any {
	out := map[string]any{}
	for k, f := range DocServiceSchema {
		out[k] = f.Default
	}
	return out
}
