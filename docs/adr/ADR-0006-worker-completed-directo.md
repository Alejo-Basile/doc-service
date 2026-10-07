# ADR-0006: El worker escribe COMPLETED directamente en MongoDB

**Estado**: Aceptada  
**Fecha**: 2026-10-04  
**Decisores**: P2 (Backend)

## Contexto

El worker de extracción (extraction-worker, P3) procesa el PDF de `raw-pdfs`,
genera el `.txt` en `extracted-txt` y debe marcar el documento como COMPLETED.
Hay dos opciones:

1. El worker llama a doc-service vía HTTP para transicionar.
2. El worker escribe directamente en MongoDB con `updateOne` condicional.

## Decisión

**Opción 2**: el worker escribe directamente en MongoDB.

### Condiciones obligatorias

1. **Transición condicional estricta**: 
   ```js
   updateOne(
     { _id: docID, status: "PROCESSING" },
     { $set: { status: "COMPLETED", txt_ref: "...", updated_at: now } }
   )
   ```
   El filtro `status: "PROCESSING"` garantiza que solo el worker que tiene el
   documento en PROCESSING pueda cerrarlo. Si otro proceso ya lo transicionó,
   el update no matchea y no hay corrupción.

2. **Rol de usuario de Mongo dedicado**: el worker usa un usuario de MongoDB
   con permisos de escritura SOLO en la colección `documents` y SOLO puede
   hacer `updateOne` con filtro de status (no insert, no delete, no update
   sin filtro de status).

3. **Verificación cruzada previa**: antes de escribir COMPLETED, el worker
   verifica que el `.txt` existe en MinIO (HEAD). Si no existe, no escribe
   COMPLETED.

4. **Reconciliador como red de seguridad**: después de escribir COMPLETED,
   el reconciliador verifica periódicamente que el `.txt` realmente existe.
   Si un documento está COMPLETED pero el `.txt` no existe, el reconciliador
   lo detecta y alerta (no lo transiciona automáticamente: eso es trabajo
   del operador).

## Consecuencias

### Positivas
- Latencia mínima: el worker no depende de doc-service para la transición final.
- Desacoplamiento: doc-service puede reiniciarse sin afectar al worker.
- Consistencia: el filtro condicional evita carreras entre worker y reconciliador.

### Negativas
- El worker necesita acceso directo a MongoDB (mayor superficie de ataque).
- La matriz de transiciones se aplica en dos lugares (doc-service y worker).
- Los tests de integración deben cubrir el camino del worker.

### Mitigaciones
- Usuario Mongo dedicado con mínimo privilegio.
- Reconciliador detecta inconsistencias COMPLETED sin .txt.
- ADR documentado y revisado por el equipo.

## Referencias

- SPEC §4: máquina de estados de la SAGA
- SPEC §11.1: transiciones condicionales
- ADR-0003: SAGA orquestada en doc-service
