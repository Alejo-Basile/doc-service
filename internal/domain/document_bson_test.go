package domain

import (
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestDocument_BSONRoundtrip verifica que la entidad Document se serializa y
// deserializa correctamente a BSON con los tags mapeados al schema v1.
func TestDocument_BSONRoundtrip(t *testing.T) {
	expires := time.Date(2026, 11, 30, 10, 45, 3, 0, time.UTC)
	created := time.Date(2026, 11, 30, 10, 15, 3, 0, time.UTC)
	updated := time.Date(2026, 11, 30, 10, 22, 10, 0, time.UTC)

	doc := &Document{
		ID:            "01J9Z8QK3M7X2V0N4P6R8T1Y0B",
		Status:        StatusCompleted,
		ObjectKey:     "raw-pdfs/01J9Z8QK3M7X2V0N4P6R8T1Y0B.pdf",
		TxtRef:        "extracted-txt/01J9Z8QK3M7X2V0N4P6R8T1Y0B.txt",
		FailureReason: "",
		CorrelationID: "corr-abc-123",
		SchemaVersion: 1,
		ExpiresAt:     expires,
		CreatedAt:     created,
		UpdatedAt:     updated,
		History: []StatusEntry{
			{Status: StatusPendingUpload, Actor: ActorDocService, CorrelationID: "corr-abc-123", At: created},
			{Status: StatusCompleted, Actor: ActorWorker, CorrelationID: "corr-abc-123", At: updated},
		},
	}

	// Serializar a BSON.
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("bson.Marshal falló: %v", err)
	}

	// Deserializar a un mapa para verificar nombres de campo.
	var m bson.M
	if err := bson.Unmarshal(raw, &m); err != nil {
		t.Fatalf("bson.Unmarshal falló: %v", err)
	}

	// Verificar que los nombres de campo BSON son los esperados (schema v1).
	// Nota: failure_reason está vacío (omitempty) y no aparece — es correcto.
	camposEsperados := []string{
		"_id", "status", "object_key", "txt_ref",
		"correlation_id", "schema_version", "expires_at", "created_at",
		"updated_at", "history",
	}
	for _, campo := range camposEsperados {
		if _, ok := m[campo]; !ok {
			t.Errorf("campo BSON %q no encontrado en el documento serializado. Keys: %v", campo, keys(m))
		}
	}
	// failure_reason vacío NO debe aparecer (omitempty).
	if _, ok := m["failure_reason"]; ok {
		t.Errorf("failure_reason no debería aparecer cuando está vacío (omitempty)")
	}

	// Verificar valores puntuales.
	if m["_id"] != doc.ID {
		t.Errorf("_id esperado %q, got %v", doc.ID, m["_id"])
	}
	if m["status"] != string(StatusCompleted) {
		t.Errorf("status esperado %q, got %v", StatusCompleted, m["status"])
	}
	if m["correlation_id"] != "corr-abc-123" {
		t.Errorf("correlation_id esperado 'corr-abc-123', got %v", m["correlation_id"])
	}
	if m["schema_version"] != int32(1) {
		t.Errorf("schema_version esperado 1, got %v", m["schema_version"])
	}

	// Verificar que history es un array con 2 entradas.
	historyArr, ok := m["history"].(bson.A)
	if !ok {
		t.Fatalf("history no es un array BSON, got %T", m["history"])
	}
	if len(historyArr) != 2 {
		t.Errorf("history longitud esperada 2, got %d", len(historyArr))
	}

	// Roundtrip completo: deserializar de vuelta a Document.
	var doc2 Document
	if err := bson.Unmarshal(raw, &doc2); err != nil {
		t.Fatalf("roundtrip Unmarshal falló: %v", err)
	}
	if doc2.ID != doc.ID || doc2.Status != doc.Status || doc2.CorrelationID != doc.CorrelationID {
		t.Errorf("roundtrip: documento deserializado difiere del original.\noriginal: %+v\ndeserializado: %+v", doc, doc2)
	}
	if len(doc2.History) != 2 {
		t.Errorf("roundtrip: history longitud esperada 2, got %d", len(doc2.History))
	}
}

// TestDocument_BSONOpcionalesVacios verifica que los campos omitempty no
// aparecen en el BSON cuando están vacíos.
func TestDocument_BSONOpcionalesVacios(t *testing.T) {
	doc := &Document{
		ID:            "01TEST",
		Status:        StatusPendingUpload,
		ObjectKey:     "",
		TxtRef:        "",
		FailureReason: "",
		CorrelationID: "corr-1",
		SchemaVersion: 1,
		ExpiresAt:     time.Now().UTC(),
		CreatedAt:     time.Now().UTC(),
		UpdatedAt:     time.Now().UTC(),
		History:       []StatusEntry{},
	}

	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatalf("bson.Marshal falló: %v", err)
	}

	var m bson.M
	if err := bson.Unmarshal(raw, &m); err != nil {
		t.Fatalf("bson.Unmarshal falló: %v", err)
	}

	// object_key, txt_ref y failure_reason están vacíos → no deben aparecer.
	for _, campo := range []string{"object_key", "txt_ref", "failure_reason"} {
		if _, ok := m[campo]; ok {
			t.Errorf("campo %q no debería aparecer en BSON cuando está vacío (omitempty)", campo)
		}
	}

	// Los campos obligatorios sí deben aparecer.
	for _, campo := range []string{"_id", "status", "correlation_id", "schema_version", "expires_at", "created_at", "updated_at", "history"} {
		if _, ok := m[campo]; !ok {
			t.Errorf("campo obligatorio %q no encontrado", campo)
		}
	}
}

// keys retorna las claves de un bson.M (helper para mensajes de error).
func keys(m bson.M) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
