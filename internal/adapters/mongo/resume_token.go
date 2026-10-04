package mongo

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// ResumeTokenStore implementa ports.ResumeTokenStore sobre MongoDB.
// Persiste el token en una colección propia para no perder eventos
// tras reinicio (SPEC §5.3, S1-P2-07).
type ResumeTokenStore struct {
	coll *mongo.Collection
}

// NewResumeTokenStore crea el store sobre la colección `change_stream_tokens`.
func NewResumeTokenStore(client *Client) *ResumeTokenStore {
	return &ResumeTokenStore{
		coll: client.Collection("change_stream_tokens"),
	}
}

// Get retorna el último resume token guardado.
// Devuelve string vacío si no hay token (primer arranque).
func (s *ResumeTokenStore) Get(ctx context.Context) (string, error) {
	var doc bson.M
	err := s.coll.FindOne(ctx, bson.M{"_id": "change_stream"}).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return "", nil
		}
		return "", fmt.Errorf("resume token read: %w", err)
	}
	token, _ := doc["token"].(string)
	return token, nil
}

// Save persiste el resume token (upsert sobre _id fijo).
func (s *ResumeTokenStore) Save(ctx context.Context, token string) error {
	_, err := s.coll.UpdateOne(ctx,
		bson.M{"_id": "change_stream"},
		bson.M{"$set": bson.M{"token": token}},
		options.UpdateOne().SetUpsert(true),
	)
	if err != nil {
		return fmt.Errorf("resume token save: %w", err)
	}
	return nil
}
