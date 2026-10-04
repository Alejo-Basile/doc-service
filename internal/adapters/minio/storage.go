// Package minio implementa el adaptador de infraestructura para MinIO/S3.
// Cumple el puerto ports.ObjectStorage.
package minio

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Config configura el cliente MinIO.
type Config struct {
	// Endpoint es el endpoint interno para el SDK (ej: minio:9000).
	Endpoint string
	// PublicEndpoint es el host público para firmar URLs (ADR-0016).
	PublicEndpoint string
	AccessKey      string
	SecretKey      string
	UseSSL         bool
	// BucketRaw es el bucket de PDFs crudos.
	BucketRaw string
	// BucketTXT es el bucket de textos extraídos.
	BucketTXT string
}

// ObjectStorage implementa ports.ObjectStorage sobre MinIO.
// Los buckets se configuran al crear el adaptador (no se pasan por llamada).
type ObjectStorage struct {
	client         *miniogo.Client
	publicEndpoint string
	bucketRaw      string
	bucketTXT      string
}

// New crea un ObjectStorage conectado a MinIO.
func New(cfg Config) (*ObjectStorage, error) {
	client, err := miniogo.New(cfg.Endpoint, &miniogo.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}

	return &ObjectStorage{
		client:         client,
		publicEndpoint: cfg.PublicEndpoint,
		bucketRaw:      cfg.BucketRaw,
		bucketTXT:      cfg.BucketTXT,
	}, nil
}

// PresignPost genera una URL prefirmada POST con política (SPEC §5.1, ADR-0011).
// La política incluye content-length-range y Content-Type.
func (o *ObjectStorage) PresignPost(
	ctx context.Context,
	objectKey string,
	opts ports.PresignPostOptions,
) (*ports.PresignPostResult, error) {
	bucket := opts.Bucket
	if bucket == "" {
		bucket = o.bucketRaw
	}

	policy := miniogo.NewPostPolicy()
	policy.SetBucket(bucket)
	policy.SetKey(objectKey)
	policy.SetExpires(time.Now().Add(opts.Expiration))

	// Content-length-range: límite de tamaño en la política (SPEC §5.1).
	if opts.MaxSizeBytes > 0 {
		policy.SetContentLengthRange(1, opts.MaxSizeBytes)
	}

	// Content-Type obligatorio si se declara.
	if opts.ContentType != "" {
		policy.SetContentType(opts.ContentType)
	}

	u, formData, err := o.client.PresignedPostPolicy(ctx, policy)
	if err != nil {
		return nil, fmt.Errorf("presign POST %s/%s: %w", bucket, objectKey, err)
	}

	uploadURL := u.String()

	// Merge de los campos del formulario con los que ya tenemos.
	fields := map[string]string{
		"key":    objectKey,
		"bucket": bucket,
	}
	for k, v := range formData {
		fields[k] = v
	}

	slog.Info("URL prefirmada POST generada",
		"object_key", objectKey,
		"bucket", bucket,
		"max_size", opts.MaxSizeBytes,
		"expires_in", opts.Expiration,
	)

	return &ports.PresignPostResult{
		UploadURL: uploadURL,
		Fields:    fields,
		ExpiresIn: opts.Expiration,
	}, nil
}

// Stat verifica la existencia de un objeto (HEAD).
// Usa el bucket raw por defecto para documentos.
func (o *ObjectStorage) Stat(ctx context.Context, objectKey string) (ports.ObjectInfo, error) {
	info, err := o.client.StatObject(ctx, o.bucketRaw, objectKey, miniogo.StatObjectOptions{})
	if err != nil {
		return ports.ObjectInfo{}, fmt.Errorf("stat %s/%s: %w", o.bucketRaw, objectKey, err)
	}

	return ports.ObjectInfo{
		Key:          objectKey,
		Size:         info.Size,
		ContentType:  info.ContentType,
		LastModified: info.LastModified,
	}, nil
}

// GetRange lee un rango de bytes del objeto (para validar %PDF-).
// start y end son inclusivos (RFC 7233). Usa el bucket raw.
func (o *ObjectStorage) GetRange(ctx context.Context, objectKey string, start, end int64) ([]byte, error) {
	opts := miniogo.GetObjectOptions{}
	opts.SetRange(start, end)

	reader, err := o.client.GetObject(ctx, o.bucketRaw, objectKey, opts)
	if err != nil {
		return nil, fmt.Errorf("get range %s/%s [%d-%d]: %w", o.bucketRaw, objectKey, start, end, err)
	}
	defer reader.Close()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("read range %s/%s: %w", o.bucketRaw, objectKey, err)
	}

	return data, nil
}

// Delete elimina un objeto del bucket raw.
func (o *ObjectStorage) Delete(ctx context.Context, objectKey string) error {
	if err := o.client.RemoveObject(ctx, o.bucketRaw, objectKey, miniogo.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete %s/%s: %w", o.bucketRaw, objectKey, err)
	}
	return nil
}

// PresignGet genera una URL prefirmada de lectura del bucket raw.
func (o *ObjectStorage) PresignGet(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	u, err := o.client.PresignedGetObject(ctx, o.bucketRaw, objectKey, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("presign GET %s/%s: %w", o.bucketRaw, objectKey, err)
	}
	return u.String(), nil
}

// PresignGetTXT genera una URL prefirmada de lectura del bucket de textos.
func (o *ObjectStorage) PresignGetTXT(ctx context.Context, objectKey string, expiry time.Duration) (string, error) {
	u, err := o.client.PresignedGetObject(ctx, o.bucketTXT, objectKey, expiry, nil)
	if err != nil {
		return "", fmt.Errorf("presign GET TXT %s/%s: %w", o.bucketTXT, objectKey, err)
	}
	return u.String(), nil
}

// Upload sube datos crudos al bucket raw (para tests y uso interno).
func (o *ObjectStorage) Upload(ctx context.Context, objectKey string, data []byte, contentType string) error {
	_, err := o.client.PutObject(ctx, o.bucketRaw, objectKey, bytes.NewReader(data), int64(len(data)), miniogo.PutObjectOptions{
		ContentType: contentType,
	})
	if err != nil {
		return fmt.Errorf("upload %s/%s: %w", o.bucketRaw, objectKey, err)
	}
	return nil
}

// BucketExists verifica si un bucket existe.
func (o *ObjectStorage) BucketExists(ctx context.Context, bucket string) (bool, error) {
	exists, err := o.client.BucketExists(ctx, bucket)
	if err != nil {
		return false, fmt.Errorf("bucket exists %s: %w", bucket, err)
	}
	return exists, nil
}

// MakeBucket crea un bucket si no existe (para tests).
func (o *ObjectStorage) MakeBucket(ctx context.Context, bucket string) error {
	err := o.client.MakeBucket(ctx, bucket, miniogo.MakeBucketOptions{})
	if err != nil {
		return fmt.Errorf("make bucket %s: %w", bucket, err)
	}
	return nil
}
