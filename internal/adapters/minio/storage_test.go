package minio

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Alejo-Basile/doc-service/internal/ports"
)

// testMinIOConfig lee el endpoint de MinIO del entorno y salta el test si no
// hay servidor: las pruebas de integración siguen necesitando un MinIO vivo
// para el setup de buckets (BucketExists/MakeBucket), no para firmar.
func testMinIOConfig(t *testing.T) (endpoint, accessKey, secretKey, publicEP string) {
	t.Helper()
	endpoint = os.Getenv("MINIO_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("MinIO no disponible: define MINIO_TEST_ENDPOINT")
	}
	accessKey = os.Getenv("MINIO_TEST_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "minioadmin"
	}
	secretKey = os.Getenv("MINIO_TEST_SECRET_KEY")
	if secretKey == "" {
		secretKey = "minioadmin123"
	}
	publicEP = os.Getenv("MINIO_TEST_PUBLIC_ENDPOINT")
	if publicEP == "" {
		// Mismo servidor alcanzable pero host distinto (localhost vs 127.0.0.1):
		// permite verificar el rewrite sin depender de DNS externo.
		host := "localhost"
		if i := strings.LastIndex(endpoint, ":"); i >= 0 {
			host = "localhost" + endpoint[i:]
		}
		publicEP = "http://" + host
	}
	return
}

// sinHostPublico es un centinela para forzar el fallback al endpoint interno,
// dado que "" no puede distinguirse de "usar el default del entorno".
const sinHostPublico = "\x00none"

// newTestStorage construye un ObjectStorage contra el MinIO de test y crea los
// buckets raw/TXT si no existen. publicEndpoint vacío usa el del entorno;
// sinHostPublico fuerza el fallback al endpoint interno.
func newTestStorage(t *testing.T, publicEndpoint string) *ObjectStorage {
	t.Helper()
	endpoint, accessKey, secretKey, envPublic := testMinIOConfig(t)
	if publicEndpoint == "" {
		publicEndpoint = envPublic
	} else if publicEndpoint == sinHostPublico {
		publicEndpoint = ""
	}
	s, err := New(Config{
		Endpoint:       endpoint,
		PublicEndpoint: publicEndpoint,
		AccessKey:      accessKey,
		SecretKey:      secretKey,
		UseSSL:         false,
		BucketRaw:      "raw-pdfs",
		BucketTXT:      "extracted-txt",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	for _, b := range []string{"raw-pdfs", "extracted-txt"} {
		exists, err := s.BucketExists(ctx, b)
		if err != nil {
			t.Fatalf("BucketExists(%s): %v", b, err)
		}
		if !exists {
			if err := s.MakeBucket(ctx, b); err != nil {
				t.Fatalf("MakeBucket(%s): %v", b, err)
			}
		}
	}
	return s
}

// decodePolicy decodifica la política base64 del formulario POST.
func decodePolicy(t *testing.T, fields map[string]string) map[string]any {
	t.Helper()
	raw, ok := fields["policy"]
	if !ok {
		t.Fatalf("el formulario no incluye el campo policy; fields=%v", fields)
	}
	jsonBytes, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("policy no es base64 válido: %v", err)
	}
	var pol map[string]any
	if err := json.Unmarshal(jsonBytes, &pol); err != nil {
		t.Fatalf("policy no es JSON válido: %v", err)
	}
	return pol
}

// TestPresignPost_IncluyeContentLengthRange verifica el límite de tamaño en la
// política (S3-P2-02, SPEC §5.1): sin él, cualquier cliente podría subir un
// archivo arbitrario.
func TestPresignPost_IncluyeContentLengthRange(t *testing.T) {
	s := newTestStorage(t, "")

	const maxSize = int64(25 * 1024 * 1024) // 25 MB
	res, err := s.PresignPost(context.Background(), "raw-pdfs/doc.pdf", ports.PresignPostOptions{
		MaxSizeBytes: maxSize,
		ContentType:  "application/pdf",
		Expiration:   15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}

	pol := decodePolicy(t, res.Fields)

	conditions, ok := pol["conditions"].([]any)
	if !ok {
		t.Fatalf("policy sin conditions: %v", pol)
	}

	var foundRange bool
	var minLen, maxLen float64
	for _, c := range conditions {
		arr, ok := c.([]any)
		if !ok || len(arr) != 3 {
			continue
		}
		if arr[0] == "content-length-range" {
			foundRange = true
			minLen = arr[1].(float64)
			maxLen = arr[2].(float64)
		}
	}
	if !foundRange {
		t.Fatalf("la política no incluye content-length-range; conditions=%v", conditions)
	}
	if minLen != 1 {
		t.Errorf("content-length-range min = %v, want 1", minLen)
	}
	if int64(maxLen) != maxSize {
		t.Errorf("content-length-range max = %v, want %d", maxLen, maxSize)
	}
}

// TestPresignPost_SinMaxSize_NoAplicaLimite comprueba que sin MaxSizeBytes no se
// añade la condición (comportamiento actual: el caller es responsable).
func TestPresignPost_SinMaxSize_NoAplicaLimite(t *testing.T) {
	s := newTestStorage(t, "")

	res, err := s.PresignPost(context.Background(), "raw-pdfs/doc.pdf", ports.PresignPostOptions{
		Expiration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}

	pol := decodePolicy(t, res.Fields)
	conditions, _ := pol["conditions"].([]any)
	for _, c := range conditions {
		if arr, ok := c.([]any); ok && len(arr) == 3 && arr[0] == "content-length-range" {
			t.Fatalf("no debía incluir content-length-range sin MaxSizeBytes")
		}
	}
}

// TestPresignPost_ContentTypeObligatorio verifica que el Content-Type declarado
// queda fijado en la política (S3-P2-02).
func TestPresignPost_ContentTypeObligatorio(t *testing.T) {
	s := newTestStorage(t, "")

	res, err := s.PresignPost(context.Background(), "raw-pdfs/doc.pdf", ports.PresignPostOptions{
		ContentType: "application/pdf",
		Expiration:  15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}

	pol := decodePolicy(t, res.Fields)
	conditions, _ := pol["conditions"].([]any)

	var foundCT bool
	for _, c := range conditions {
		arr, ok := c.([]any)
		if ok && len(arr) == 3 && arr[0] == "eq" && arr[1] == "$Content-Type" && arr[2] == "application/pdf" {
			foundCT = true
		}
	}
	if !foundCT {
		t.Fatalf("la política no fija Content-Type=application/pdf; conditions=%v", conditions)
	}
}

// TestPresignPost_KeyYBucketDerivados verifica que key/bucket del formulario
// coinciden con lo solicitado (el cliente S3 los reenvía tal cual).
func TestPresignPost_KeyYBucketDerivados(t *testing.T) {
	s := newTestStorage(t, "")

	res, err := s.PresignPost(context.Background(), "raw-pdfs/abc.pdf", ports.PresignPostOptions{
		Expiration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}

	if res.Fields["key"] != "raw-pdfs/abc.pdf" {
		t.Errorf("field key = %q, want raw-pdfs/abc.pdf", res.Fields["key"])
	}
	if res.Fields["bucket"] != "raw-pdfs" {
		t.Errorf("field bucket = %q, want raw-pdfs (default)", res.Fields["bucket"])
	}
}

// TestPresignPost_BucketExplicito verifica que se respeta opts.Bucket.
func TestPresignPost_BucketExplicito(t *testing.T) {
	s := newTestStorage(t, "")

	res, err := s.PresignPost(context.Background(), "extracted-txt/x.txt", ports.PresignPostOptions{
		Bucket:     "extracted-txt",
		Expiration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}
	if res.Fields["bucket"] != "extracted-txt" {
		t.Errorf("field bucket = %q, want extracted-txt", res.Fields["bucket"])
	}
}

// internalHostExtrae retorna el host:puerto del endpoint interno configurado
// para poder aseverar que NO se filtra en las URLs públicas.
func internalHostExtrae(t *testing.T) string {
	t.Helper()
	return os.Getenv("MINIO_TEST_ENDPOINT")
}

// TestPresignPost_HostPublico verifica que la UploadURL se reescribe al host
// público (ADR-0016): el navegador no debe recibir el endpoint interno.
func TestPresignPost_HostPublico(t *testing.T) {
	_, _, _, publicEP := testMinIOConfig(t)
	s := newTestStorage(t, publicEP)

	res, err := s.PresignPost(context.Background(), "raw-pdfs/doc.pdf", ports.PresignPostOptions{
		Expiration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}

	if !strings.HasPrefix(res.UploadURL, publicEP+"/") {
		t.Errorf("UploadURL = %q, want prefijo host público %q", res.UploadURL, publicEP)
	}
	// El endpoint interno (127.0.0.1) NO debe filtrarse: el público usa localhost.
	if strings.Contains(res.UploadURL, "127.0.0.1") {
		t.Errorf("UploadURL filtra el endpoint interno: %q", res.UploadURL)
	}
}

// TestPresignPost_SinHostPublico_UsaInterno verifica el fallback: sin
// public_endpoint, la URL queda con el endpoint interno.
func TestPresignPost_SinHostPublico_UsaInterno(t *testing.T) {
	internal := internalHostExtrae(t)
	s := newTestStorage(t, sinHostPublico) // fuerza fallback al interno

	res, err := s.PresignPost(context.Background(), "raw-pdfs/doc.pdf", ports.PresignPostOptions{
		Expiration: 15 * time.Minute,
	})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}
	if !strings.Contains(res.UploadURL, internal) {
		t.Errorf("UploadURL = %q, want endpoint interno %q", res.UploadURL, internal)
	}
}

// TestPresignGet_HostPublico verifica que las URLs GET también se reescriben.
func TestPresignGet_HostPublico(t *testing.T) {
	_, _, _, publicEP := testMinIOConfig(t)
	s := newTestStorage(t, publicEP)

	u, err := s.PresignGet(context.Background(), "raw-pdfs/doc.pdf", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if !strings.HasPrefix(u, publicEP+"/") {
		t.Errorf("PresignGet URL = %q, want prefijo host público %q", u, publicEP)
	}
	if strings.Contains(u, "127.0.0.1") {
		t.Errorf("PresignGet URL filtra el endpoint interno: %q", u)
	}
}

// TestPresignGetTXT_UsaBucketTXT verifica que la URL TXT apunta al bucket de
// textos extraídos y respeta el host público.
func TestPresignGetTXT_UsaBucketTXT(t *testing.T) {
	_, _, _, publicEP := testMinIOConfig(t)
	s := newTestStorage(t, publicEP)

	u, err := s.PresignGetTXT(context.Background(), "abc.txt", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGetTXT: %v", err)
	}
	if !strings.Contains(u, "/extracted-txt/abc.txt") {
		t.Errorf("PresignGetTXT URL = %q, want path /extracted-txt/abc.txt", u)
	}
	if !strings.HasPrefix(u, publicEP+"/") {
		t.Errorf("PresignGetTXT URL = %q, want prefijo host público %q", u, publicEP)
	}
}

// TestNew_PublicEndpointInvalido verifica el error de parseo temprano.
func TestNew_PublicEndpointInvalido(t *testing.T) {
	_, err := New(Config{
		Endpoint:       "minio:9000",
		PublicEndpoint: "http://[::1]:namedport",
		AccessKey:      "a",
		SecretKey:      "b",
		BucketRaw:      "raw-pdfs",
	})
	if err == nil {
		t.Fatal("New error = nil, want error con public endpoint inválido")
	}
}

// credentialRegion parsea un valor de credencial (URL o form field) y devuelve
// el segmento de región (índice 2 al separar por "/").
func credentialRegion(t *testing.T, raw string) string {
	t.Helper()
	// raw puede ser una URL completa o un valor tipo "access/date/region/s3/aws4_request".
	v := raw
	if strings.HasPrefix(raw, "http") {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatalf("parse url: %v", err)
		}
		v = u.Query().Get("X-Amz-Credential")
	}
	parts := strings.Split(v, "/")
	if len(parts) < 3 {
		t.Fatalf("credential con forma inesperada: %q", v)
	}
	return parts[2]
}

func newOfflineStorage(t *testing.T, region string) *ObjectStorage {
	t.Helper()
	s, err := New(Config{
		Endpoint:       "127.0.0.1:1",
		PublicEndpoint: "http://127.0.0.1:1",
		AccessKey:      "minioadmin",
		SecretKey:      "minioadmin123",
		BucketRaw:      "raw-pdfs",
		BucketTXT:      "extracted-txt",
		Region:         region,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return s
}

func TestPresign_SinRed_RegionPorDefecto(t *testing.T) {
	s := newOfflineStorage(t, "")
	ctx := context.Background()
	u, err := s.PresignGet(ctx, "raw-pdfs/doc.pdf", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	pu, _ := url.Parse(u)
	if pu.Host != "127.0.0.1:1" {
		t.Errorf("host = %q", pu.Host)
	}
	if got := credentialRegion(t, u); got != "us-east-1" {
		t.Errorf("region = %q, want us-east-1", got)
	}
	u2, err := s.PresignGetTXT(ctx, "abc.txt", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGetTXT: %v", err)
	}
	if !strings.Contains(u2, "/extracted-txt/abc.txt") {
		t.Errorf("path = %q", u2)
	}
	if got := credentialRegion(t, u2); got != "us-east-1" {
		t.Errorf("region = %q, want us-east-1", got)
	}
	res, err := s.PresignPost(ctx, "raw-pdfs/doc.pdf", ports.PresignPostOptions{Expiration: 15 * time.Minute})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}
	if got := credentialRegion(t, res.Fields["x-amz-credential"]); got != "us-east-1" {
		t.Errorf("region = %q, want us-east-1", got)
	}
}

func TestPresign_RegionPersonalizada(t *testing.T) {
	s := newOfflineStorage(t, "eu-west-1")
	ctx := context.Background()
	u, err := s.PresignGet(ctx, "raw-pdfs/doc.pdf", 5*time.Minute)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	if got := credentialRegion(t, u); got != "eu-west-1" {
		t.Errorf("region = %q, want eu-west-1", got)
	}
	res, err := s.PresignPost(ctx, "raw-pdfs/doc.pdf", ports.PresignPostOptions{Expiration: 15 * time.Minute})
	if err != nil {
		t.Fatalf("PresignPost: %v", err)
	}
	if got := credentialRegion(t, res.Fields["x-amz-credential"]); got != "eu-west-1" {
		t.Errorf("region = %q, want eu-west-1", got)
	}
}

func TestPresignGet_Concurrente_SinRed(t *testing.T) {
	s := newOfflineStorage(t, "")
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 64)
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.PresignGet(ctx, "raw-pdfs/doc.pdf", 5*time.Minute); err != nil {
				errs <- err
			}
			if _, err := s.PresignGetTXT(ctx, "abc.txt", 5*time.Minute); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("err: %v", e)
	}
}
