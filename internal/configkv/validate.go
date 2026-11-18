package configkv

import "encoding/json"

func Decode(raw []byte) (map[string]any, []Rejection) {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return map[string]any{}, []Rejection{{Key: "", Reason: "malformed_json"}}
	}
	out := map[string]any{}
	for k, v := range m {
		if len(k) > 0 && k[0] == '_' {
			continue
		}
		out[k] = v
	}
	return out, nil
}

func isInt(v any) bool {
	f, ok := v.(float64)
	if !ok {
		return false
	}
	return f == float64(int64(f))
}

// normalize coerces a value already known to be valid into the canonical Go
// type for its field, so coherence arithmetic never sees a float64 where an
// int64 is expected.
func normalize(f Field, v any) any {
	switch f.Type {
	case "int":
		switch n := v.(type) {
		case int64:
			return n
		case int:
			return int64(n)
		case float64:
			return int64(n)
		}
	case "bool":
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return v
}

func Apply(current map[string]any, incoming map[string]any) (map[string]any, []Rejection) {
	if current == nil {
		current = Defaults()
	}
	eff := map[string]any{}
	for k, v := range current {
		eff[k] = normalize(DocServiceSchema[k], v)
	}
	var rejections []Rejection
	for k, v := range incoming {
		f, ok := DocServiceSchema[k]
		if !ok {
			rejections = append(rejections, Rejection{Key: k, Reason: "unknown_key"})
			continue
		}
		if f.Type == "int" {
			if !isInt(v) {
				rejections = append(rejections, Rejection{Key: k, Reason: "type"})
				continue
			}
		} else if f.Type == "bool" {
			if _, ok := v.(bool); !ok {
				rejections = append(rejections, Rejection{Key: k, Reason: "type"})
				continue
			}
		}
		if f.HasRange && f.Type == "int" {
			nv := int64(v.(float64))
			if float64(nv) < f.Min || float64(nv) > f.Max {
				rejections = append(rejections, Rejection{Key: k, Reason: "range"})
				continue
			}
		}
		if f.Type == "int" {
			eff[k] = int64(v.(float64))
		} else {
			eff[k] = v
		}
	}
	// coherence
	grace, _ := eff["doc_upload_grace_minutes"].(int64)
	recon, _ := eff["reconcile_interval_minutes"].(int64)
	broken := ""
	if grace < 2*recon {
		broken = "doc_upload_grace_minutes"
	}
	minSafety, _ := eff["min_safety_age_minutes"].(int64)
	if broken == "" && minSafety < recon {
		broken = "min_safety_age_minutes"
	}
	if broken != "" {
		if _, in := incoming[broken]; in {
			rejections = append(rejections, Rejection{Key: broken, Reason: "coherence"})
			if cv, ok := current[broken]; ok {
				eff[broken] = cv
			} else {
				eff[broken] = DocServiceSchema[broken].Default
			}
		} else {
			for _, cand := range []string{"doc_upload_grace_minutes", "min_safety_age_minutes", "reconcile_interval_minutes"} {
				if _, in := incoming[cand]; in {
					rejections = append(rejections, Rejection{Key: cand, Reason: "coherence"})
					if cv, ok := current[cand]; ok {
						eff[cand] = cv
					}
					break
				}
			}
		}
	}
	return eff, rejections
}
